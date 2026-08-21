package auth

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/mulgadc/bluebottle/pkg/sigv4"

	"github.com/mulgadc/goanna/internal/cloudwatch"
)

// ServiceName is the SigV4 service the AWS SDKs sign CloudWatch requests
// with. It is "monitoring", not "cloudwatch", and it is folded into the
// signing key — signing under any other name cannot verify.
const ServiceName = "monitoring"

// Resolver turns an access key ID into the secret and account behind it. The
// middleware takes the interface rather than *Provider so the gate can be
// exercised without a live IAM store.
type Resolver interface {
	Lookup(ctx context.Context, accessKeyID string) (*Credential, error)
}

var _ Resolver = (*Provider)(nil)

// MiddlewareOptions configures the SigV4 gate.
type MiddlewareOptions struct {
	Provider Resolver
	// Region the client must have signed with.
	Region string
	Logger *slog.Logger
}

// Middleware verifies the SigV4 signature on every request and attaches the
// resolved account to the context. A handler downstream can obtain a caller's
// account only from here, never from request content.
func Middleware(opts MiddlewareOptions) func(http.Handler) http.Handler {
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Parse reads, hashes and rewinds the body to rebuild the
			// canonical request, so the handler can still read the form.
			signed, err := sigv4.Parse(r)
			if err != nil {
				writeParseError(w, log, r, err)
				return
			}

			// Reject an unexpected service before any crypto: Verify re-signs
			// with the client-claimed service name, so accepting it here would
			// rubber-stamp the scope.
			if signed.Credential.Service != ServiceName {
				log.Warn("auth failure: unexpected service in credential scope",
					"access_key_id", signed.Credential.AccessKeyID,
					"service", signed.Credential.Service)
				cloudwatch.WriteAuthError(w, r, log, "SignatureDoesNotMatch",
					"The request signature we calculated does not match the signature you provided.",
					http.StatusForbidden)
				return
			}

			cred, err := opts.Provider.Lookup(r.Context(), signed.Credential.AccessKeyID)
			if err != nil {
				writeLookupError(w, r, log, signed.Credential.AccessKeyID, err)
				return
			}

			if _, err := signed.Verify(cred.SecretAccessKey, opts.Region, ServiceName); err != nil {
				// A mismatch means our canonical request differs from the one
				// the client signed. Log ours so the two can be diffed; it
				// carries signed header values and a payload hash, never the
				// secret.
				log.Warn("auth failure: signature verification failed",
					"access_key_id", signed.Credential.AccessKeyID,
					"region", signed.Credential.Region,
					"canonical_request", signed.CanonicalRequest(),
					"error", err)
				cloudwatch.WriteAuthError(w, r, log, "SignatureDoesNotMatch",
					"The request signature we calculated does not match the signature you provided. "+
						"Check your AWS Secret Access Key and signing method.",
					http.StatusForbidden)
				return
			}

			ctx := cloudwatch.WithIdentity(r.Context(), cloudwatch.Identity{
				AccountID:   cred.AccountID,
				AccessKeyID: signed.Credential.AccessKeyID,
				UserName:    cred.UserName,
			})
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// writeParseError maps a signing-envelope rejection onto an AWS error code.
// sigv4 validates the envelope, credential scope and timestamp at parse time,
// so these are all pre-credential failures.
func writeParseError(w http.ResponseWriter, log *slog.Logger, r *http.Request, err error) {
	switch {
	case errors.Is(err, sigv4.ErrMissingAuthentication):
		cloudwatch.WriteAuthError(w, r, log, "MissingAuthenticationToken",
			"Request is missing Authentication Token.", http.StatusForbidden)
	case errors.Is(err, sigv4.ErrPayloadTooLarge):
		cloudwatch.WriteAuthError(w, r, log, "RequestEntityTooLarge",
			"The request payload is too large.", http.StatusRequestEntityTooLarge)
	case errors.Is(err, sigv4.ErrRequestTimeTooSkewed):
		// AWS reports skew as SignatureDoesNotMatch, which is
		// indistinguishable on the wire from a canonicalisation mismatch. Log
		// the distinction so the two can be told apart afterwards.
		log.Warn("auth failure: request time too skewed",
			"remote_addr", r.RemoteAddr,
			"max_skew_ms", sigv4.MaxClockSkew.Milliseconds())
		cloudwatch.WriteAuthError(w, r, log, "SignatureDoesNotMatch",
			"Signature expired or not yet current.", http.StatusForbidden)
	default:
		log.Warn("auth failure: malformed signature envelope",
			"remote_addr", r.RemoteAddr, "error", err)
		cloudwatch.WriteAuthError(w, r, log, "IncompleteSignature",
			"The request signature does not conform to AWS standards.", http.StatusBadRequest)
	}
}

// writeLookupError keeps an unknown, inactive and undecryptable key
// indistinguishable to the caller while separating them from an IAM outage,
// which must not look like a bad credential.
func writeLookupError(w http.ResponseWriter, r *http.Request, log *slog.Logger,
	accessKeyID string, err error) {
	switch {
	case errors.Is(err, ErrExpired):
		cloudwatch.WriteAuthError(w, r, log, "ExpiredToken",
			"The security token included in the request is expired.", http.StatusForbidden)
	case errors.Is(err, ErrKeyNotFound):
		log.Warn("auth failure: access key not resolvable", "access_key_id", accessKeyID)
		cloudwatch.WriteAuthError(w, r, log, "InvalidClientTokenId",
			"The security token included in the request is invalid.", http.StatusForbidden)
	case errors.Is(err, ErrUnavailable):
		log.Error("credential lookup unavailable", "access_key_id", accessKeyID, "error", err)
		cloudwatch.WriteAuthError(w, r, log, "ServiceUnavailable",
			"The credential store is temporarily unavailable.", http.StatusServiceUnavailable)
	default:
		log.Error("credential lookup failed", "access_key_id", accessKeyID, "error", err)
		cloudwatch.WriteAuthError(w, r, log, "InternalFailure",
			"The request processing has failed because of an unknown error.",
			http.StatusInternalServerError)
	}
}
