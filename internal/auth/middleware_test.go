package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"

	"github.com/mulgadc/goanna/internal/cloudwatch"
)

const (
	testRegion    = "ap-southeast-2"
	testAccessKey = "AKIAIOSFODNN7EXAMPLE"
	testSecret    = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	testAccountID = "111122223333"
)

// fakeResolver answers with a fixed credential, or with whatever error the
// test wants the gate to classify.
type fakeResolver struct {
	cred *Credential
	err  error
}

func (f fakeResolver) Lookup(context.Context, string) (*Credential, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.cred, nil
}

func okResolver() fakeResolver {
	return fakeResolver{cred: &Credential{
		SecretAccessKey: testSecret,
		AccountID:       testAccountID,
		UserName:        "tester",
	}}
}

// gate wraps a handler that records the identity the middleware resolved.
func gate(resolver Resolver) (http.Handler, *cloudwatch.Identity) {
	seen := new(cloudwatch.Identity)
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if id, ok := cloudwatch.IdentityFrom(r.Context()); ok {
			*seen = id
		}
		w.WriteHeader(http.StatusOK)
	})
	mw := Middleware(MiddlewareOptions{Provider: resolver, Region: testRegion})
	return mw(next), seen
}

// signedRequest builds a CloudWatch query-protocol POST signed the way the AWS
// SDK signs one. Signing with the SDK rather than by hand is the point: it
// proves a real client's signature verifies end to end.
func signedRequest(t *testing.T, opts ...func(*signOptions)) *http.Request {
	t.Helper()
	cfg := signOptions{
		accessKey: testAccessKey,
		secret:    testSecret,
		service:   ServiceName,
		region:    testRegion,
		when:      time.Now().UTC(),
		body:      url.Values{"Action": {"ListMetrics"}, "Version": {"2010-08-01"}}.Encode(),
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	req, err := http.NewRequest(http.MethodPost, "https://goanna.internal:8444/", strings.NewReader(cfg.body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	sum := sha256.Sum256([]byte(cfg.body))
	signer := v4.NewSigner()
	creds := aws.Credentials{AccessKeyID: cfg.accessKey, SecretAccessKey: cfg.secret}
	if err := signer.SignHTTP(context.Background(), creds, req,
		hex.EncodeToString(sum[:]), cfg.service, cfg.region, cfg.when); err != nil {
		t.Fatalf("sign: %v", err)
	}

	if cfg.tamperBody != "" {
		req.Body = http.NoBody
		req, err = http.NewRequest(req.Method, req.URL.String(), strings.NewReader(cfg.tamperBody))
		if err != nil {
			t.Fatalf("rebuild request: %v", err)
		}
		req.Header = signedHeaders(t, cfg)
	}
	return req
}

type signOptions struct {
	accessKey  string
	secret     string
	service    string
	region     string
	when       time.Time
	body       string
	tamperBody string
}

// signedHeaders re-signs the original body and returns those headers, so the
// caller can attach them to a different body.
func signedHeaders(t *testing.T, cfg signOptions) http.Header {
	t.Helper()
	clean := cfg
	clean.tamperBody = ""
	original := signedRequest(t, func(o *signOptions) { *o = clean })
	return original.Header
}

func withAccessKey(id string) func(*signOptions) {
	return func(o *signOptions) { o.accessKey = id }
}

func withSecret(secret string) func(*signOptions) {
	return func(o *signOptions) { o.secret = secret }
}

func withService(service string) func(*signOptions) {
	return func(o *signOptions) { o.service = service }
}

func withRegion(region string) func(*signOptions) {
	return func(o *signOptions) { o.region = region }
}

func withTime(when time.Time) func(*signOptions) {
	return func(o *signOptions) { o.when = when }
}

func errorCode(t *testing.T, body string) string {
	t.Helper()
	var resp struct {
		Error struct {
			Code string `xml:"Code"`
		} `xml:"Error"`
	}
	if err := xml.Unmarshal([]byte(body), &resp); err != nil {
		t.Fatalf("unmarshal %q: %v", body, err)
	}
	return resp.Error.Code
}

func serve(t *testing.T, handler http.Handler, req *http.Request) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec.Code, rec.Body.String()
}

// An AWS SDK signature must verify, and the account must reach the handler.
func TestMiddlewareAcceptsAnSDKSignature(t *testing.T) {
	handler, seen := gate(okResolver())

	status, body := serve(t, handler, signedRequest(t))
	if status != http.StatusOK {
		t.Fatalf("status = %d, body = %s", status, body)
	}
	if seen.AccountID != testAccountID {
		t.Errorf("account = %q, want %q", seen.AccountID, testAccountID)
	}
	if seen.AccessKeyID != testAccessKey || seen.UserName != "tester" {
		t.Errorf("identity = %+v", *seen)
	}
}

// The handler must still be able to read the form: sigv4 buffers the body to
// hash it, and has to put it back.
func TestMiddlewareLeavesTheBodyReadable(t *testing.T) {
	var action string
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Errorf("parse form: %v", err)
		}
		action = r.Form.Get("Action")
	})
	mw := Middleware(MiddlewareOptions{Provider: okResolver(), Region: testRegion})

	if _, body := serve(t, mw(next), signedRequest(t)); body != "" {
		t.Fatalf("unexpected body: %s", body)
	}
	if action != "ListMetrics" {
		t.Errorf("action = %q, want ListMetrics", action)
	}
}

func TestMiddlewareRejections(t *testing.T) {
	tests := []struct {
		name     string
		resolver Resolver
		request  func(*testing.T) *http.Request
		status   int
		code     string
	}{
		{
			name:     "no signature at all",
			resolver: okResolver(),
			request: func(t *testing.T) *http.Request {
				return httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=ListMetrics"))
			},
			status: http.StatusForbidden,
			code:   "MissingAuthenticationToken",
		},
		{
			name:     "a malformed Authorization header",
			resolver: okResolver(),
			request: func(t *testing.T) *http.Request {
				req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(""))
				req.Header.Set("Authorization", "AWS4-HMAC-SHA256 nonsense")
				return req
			},
			status: http.StatusBadRequest,
			code:   "IncompleteSignature",
		},
		{
			// Verify re-signs with the client-claimed service, so accepting an
			// unexpected one would rubber-stamp the scope.
			name:     "signed for another service",
			resolver: okResolver(),
			request:  func(t *testing.T) *http.Request { return signedRequest(t, withService("ec2")) },
			status:   http.StatusForbidden,
			code:     "SignatureDoesNotMatch",
		},
		{
			name:     "signed for another region",
			resolver: okResolver(),
			request:  func(t *testing.T) *http.Request { return signedRequest(t, withRegion("us-east-1")) },
			status:   http.StatusForbidden,
			code:     "SignatureDoesNotMatch",
		},
		{
			name:     "signed with the wrong secret",
			resolver: okResolver(),
			request:  func(t *testing.T) *http.Request { return signedRequest(t, withSecret("not-the-secret")) },
			status:   http.StatusForbidden,
			code:     "SignatureDoesNotMatch",
		},
		{
			name:     "signed an hour ago",
			resolver: okResolver(),
			request: func(t *testing.T) *http.Request {
				return signedRequest(t, withTime(time.Now().UTC().Add(-time.Hour)))
			},
			status: http.StatusForbidden,
			code:   "SignatureDoesNotMatch",
		},
		{
			name:     "an unknown access key",
			resolver: fakeResolver{err: fmt.Errorf("%w: nope", ErrKeyNotFound)},
			request:  func(t *testing.T) *http.Request { return signedRequest(t, withAccessKey("AKIAUNKNOWNKEY000000")) },
			status:   http.StatusForbidden,
			code:     "InvalidClientTokenId",
		},
		{
			name:     "an expired session credential",
			resolver: fakeResolver{err: fmt.Errorf("%w: nope", ErrExpired)},
			request:  func(t *testing.T) *http.Request { return signedRequest(t, withAccessKey("ASIAEXPIRED000000000")) },
			status:   http.StatusForbidden,
			code:     "ExpiredToken",
		},
		{
			// An IAM outage must not look like a bad credential, or a client
			// throws away working keys.
			name:     "the credential store is down",
			resolver: fakeResolver{err: fmt.Errorf("%w: nats down", ErrUnavailable)},
			request:  func(t *testing.T) *http.Request { return signedRequest(t) },
			status:   http.StatusServiceUnavailable,
			code:     "ServiceUnavailable",
		},
		{
			name:     "an unclassified lookup failure",
			resolver: fakeResolver{err: errors.New("boom")},
			request:  func(t *testing.T) *http.Request { return signedRequest(t) },
			status:   http.StatusInternalServerError,
			code:     "InternalFailure",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, seen := gate(tt.resolver)
			status, body := serve(t, handler, tt.request(t))
			if status != tt.status {
				t.Errorf("status = %d, want %d (%s)", status, tt.status, body)
			}
			if got := errorCode(t, body); got != tt.code {
				t.Errorf("code = %s, want %s", got, tt.code)
			}
			if seen.AccountID != "" {
				t.Errorf("a rejected request still reached the handler as %+v", *seen)
			}
		})
	}
}

// A body swapped after signing must not verify, or the signature protects
// nothing but the headers.
func TestMiddlewareRejectsATamperedBody(t *testing.T) {
	handler, _ := gate(okResolver())
	req := signedRequest(t, func(o *signOptions) {
		o.tamperBody = url.Values{"Action": {"PutMetricData"}}.Encode()
	})

	status, body := serve(t, handler, req)
	if status != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (%s)", status, body)
	}
	if got := errorCode(t, body); got != "SignatureDoesNotMatch" {
		t.Errorf("code = %s, want SignatureDoesNotMatch", got)
	}
}

// The service name is folded into the signing key, so getting it wrong fails
// every request. Pinning it here catches a rename that would look harmless.
func TestServiceNameIsMonitoring(t *testing.T) {
	if ServiceName != "monitoring" {
		t.Errorf("ServiceName = %q; the AWS SDKs sign CloudWatch as \"monitoring\"", ServiceName)
	}
}
