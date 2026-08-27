// Package auth resolves spinifex-issued access keys so a SigV4 signature can
// be verified against the secret behind them.
package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/mulgadc/bluebottle/pkg/masterkey"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Errors callers distinguish. Everything else is an infrastructure fault and
// must surface as a server error, not a rejected credential.
var (
	// ErrKeyNotFound covers an unknown, inactive or deleted access key. The
	// three are deliberately indistinguishable to the caller.
	ErrKeyNotFound = errors.New("auth: access key not found")
	// ErrExpired is a session credential past its expiry.
	ErrExpired = errors.New("auth: session credential expired")
	// ErrUnavailable means the IAM store could not be reached.
	ErrUnavailable = errors.New("auth: credential store unavailable")
)

// KV buckets spinifex publishes IAM state into.
const (
	defaultAccessKeysBucket = "spinifex-iam-access-keys"
	//nolint:gosec // G101: bucket name, not a credential value
	sessionCredentialsBucket = "spinifex-iam-session-credentials"
)

// Access key ID prefixes. Long-lived IAM keys and STS session credentials live
// in disjoint buckets, so dispatching on the prefix first means a misfiled
// record cannot be resolved by the wrong lookup path.
const (
	longLivedPrefix = "AKIA"
	sessionPrefix   = "ASIA"
)

// cacheTTL bounds how long a deactivated long-lived key keeps working if the
// KV watcher is not running. With the watcher it is only a backstop.
const cacheTTL = 60 * time.Second

// Credential is what a verified signature needs plus the account it binds to.
type Credential struct {
	SecretAccessKey string
	AccountID       string
	UserName        string
}

// accessKeyRecord mirrors the fields goanna needs from spinifex's IAM
// AccessKey. The contract across this boundary is the JSON in KV, so only the
// fields used here are replicated.
type accessKeyRecord struct {
	SecretAccessKey string `json:"secret_access_key"` // AES-256-GCM, base64
	UserName        string `json:"user_name"`
	AccountID       string `json:"account_id"`
	Status          string `json:"status"`
}

// sessionRecord mirrors the fields goanna needs from an STS session
// credential.
type sessionRecord struct {
	SecretEncrypted string    `json:"secret_encrypted"`
	AccountID       string    `json:"account_id"`
	SessionName     string    `json:"session_name"`
	ExpiresAt       time.Time `json:"expires_at"`
}

const accessKeyStatusActive = "Active"

// Config configures a Provider.
type Config struct {
	// MasterKeyPath is the shared IAM key, group-readable at
	// /etc/spinifex/master.key on a deployed node.
	MasterKeyPath    string
	AccessKeysBucket string
	Logger           *slog.Logger
}

type cachedCredential struct {
	cred      *Credential
	expiresAt time.Time
}

// Provider resolves an access key ID to the secret and account behind it.
//
// Buckets are opened lazily: goannad may well start before the spinifex daemon
// has created them, and that must not be fatal.
type Provider struct {
	js     jetstream.JetStream
	key    *masterkey.Key
	log    *slog.Logger
	bucket string

	mu    sync.RWMutex
	cache map[string]cachedCredential

	accessKeys   jetstream.KeyValue
	sessions     jetstream.KeyValue
	watcher      jetstream.KeyWatcher
	watcherStop  chan struct{}
	watcherOnce  sync.Once
	closedSignal sync.Once
}

// NewProvider builds a Provider over an existing NATS connection. goannad has
// one already for ingest, and a second would only double the reconnect
// handling and the token handling.
func NewProvider(nc *nats.Conn, cfg Config) (*Provider, error) {
	if nc == nil {
		return nil, errors.New("auth: a NATS connection is required")
	}
	if cfg.MasterKeyPath == "" {
		return nil, errors.New("auth: master key path is required")
	}

	// The IAM master key is shared across services on the host via group
	// ownership, so the shared loader is the right one — the strict 0600
	// loader would reject the deployed file.
	key, err := masterkey.LoadShared(cfg.MasterKeyPath)
	if err != nil {
		return nil, fmt.Errorf("auth: load master key: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("auth: jetstream context: %w", err)
	}

	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	bucket := cfg.AccessKeysBucket
	if bucket == "" {
		bucket = defaultAccessKeysBucket
	}

	p := &Provider{
		js:          js,
		key:         key,
		log:         log,
		bucket:      bucket,
		cache:       map[string]cachedCredential{},
		watcherStop: make(chan struct{}),
	}
	if err := p.ensureAccessKeys(context.Background()); err != nil {
		p.log.Warn("IAM access-keys bucket not available yet; authentication "+
			"activates once the spinifex daemon creates it", "error", err)
	}
	return p, nil
}

// Lookup resolves accessKeyID. The prefix decides which store answers.
func (p *Provider) Lookup(ctx context.Context, accessKeyID string) (*Credential, error) {
	switch {
	case strings.HasPrefix(accessKeyID, sessionPrefix):
		return p.lookupSession(ctx, accessKeyID)
	case strings.HasPrefix(accessKeyID, longLivedPrefix):
		return p.lookupAccessKey(ctx, accessKeyID)
	default:
		return nil, fmt.Errorf("%w: unrecognised key prefix", ErrKeyNotFound)
	}
}

func (p *Provider) lookupAccessKey(ctx context.Context, accessKeyID string) (*Credential, error) {
	p.mu.RLock()
	cached, ok := p.cache[accessKeyID]
	p.mu.RUnlock()
	if ok && time.Now().Before(cached.expiresAt) {
		return cached.cred, nil
	}

	if err := p.ensureAccessKeys(ctx); err != nil {
		return nil, err
	}

	entry, err := p.accessKeys.Get(ctx, accessKeyID)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, fmt.Errorf("%w: %s", ErrKeyNotFound, accessKeyID)
		}
		return nil, fmt.Errorf("%w: get access key: %w", ErrUnavailable, err)
	}

	var rec accessKeyRecord
	if err := json.Unmarshal(entry.Value(), &rec); err != nil {
		return nil, fmt.Errorf("auth: unmarshal access key %s: %w", accessKeyID, err)
	}
	if rec.Status != accessKeyStatusActive {
		p.log.Warn("authentication attempt with inactive access key",
			"access_key_id", accessKeyID, "account_id", rec.AccountID, "status", rec.Status)
		return nil, fmt.Errorf("%w: %s is not active", ErrKeyNotFound, accessKeyID)
	}
	// An empty account would scope every query to the series carrying no
	// account label at all, so it is rejected at the boundary.
	if rec.AccountID == "" {
		p.log.Error("access key has no account_id; refusing to authenticate",
			"access_key_id", accessKeyID, "user_name", rec.UserName)
		return nil, fmt.Errorf("%w: %s has no account", ErrKeyNotFound, accessKeyID)
	}

	secret, err := p.key.DecryptBase64(rec.SecretAccessKey)
	if err != nil {
		// An undecryptable secret (a rotated master key, say) is an
		// authentication failure, not a server fault: retrying will not help,
		// re-issuing the key will.
		p.log.Error("failed to decrypt IAM secret", "access_key_id", accessKeyID, "error", err)
		return nil, fmt.Errorf("%w: %s cannot be decrypted", ErrKeyNotFound, accessKeyID)
	}

	cred := &Credential{SecretAccessKey: secret, AccountID: rec.AccountID, UserName: rec.UserName}
	p.mu.Lock()
	p.cache[accessKeyID] = cachedCredential{cred: cred, expiresAt: time.Now().Add(cacheTTL)}
	p.mu.Unlock()
	return cred, nil
}

// lookupSession resolves an STS session credential. Results are never cached
// so expiry is re-checked on every request.
func (p *Provider) lookupSession(ctx context.Context, accessKeyID string) (*Credential, error) {
	if err := p.ensureSessions(ctx); err != nil {
		return nil, err
	}

	entry, err := p.sessions.Get(ctx, accessKeyID)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, fmt.Errorf("%w: %s", ErrKeyNotFound, accessKeyID)
		}
		return nil, fmt.Errorf("%w: get session credential: %w", ErrUnavailable, err)
	}

	var rec sessionRecord
	if err := json.Unmarshal(entry.Value(), &rec); err != nil {
		return nil, fmt.Errorf("auth: unmarshal session credential %s: %w", accessKeyID, err)
	}
	// Expired records stay readable until the STS janitor reaps them.
	if time.Now().UTC().After(rec.ExpiresAt) {
		return nil, fmt.Errorf("%w: %s", ErrExpired, accessKeyID)
	}
	if rec.AccountID == "" {
		p.log.Error("session credential has no account_id; refusing to authenticate",
			"access_key_id", accessKeyID, "session_name", rec.SessionName)
		return nil, fmt.Errorf("%w: %s has no account", ErrKeyNotFound, accessKeyID)
	}

	secret, err := p.key.DecryptBase64(rec.SecretEncrypted)
	if err != nil {
		p.log.Error("failed to decrypt session secret", "access_key_id", accessKeyID, "error", err)
		return nil, fmt.Errorf("%w: %s cannot be decrypted", ErrKeyNotFound, accessKeyID)
	}
	return &Credential{
		SecretAccessKey: secret,
		AccountID:       rec.AccountID,
		UserName:        rec.SessionName,
	}, nil
}

// ensureAccessKeys opens the access-keys bucket and starts the cache watcher.
func (p *Provider) ensureAccessKeys(ctx context.Context) error {
	p.mu.RLock()
	ready := p.accessKeys != nil
	p.mu.RUnlock()
	if ready {
		return nil
	}

	kv, err := p.js.KeyValue(ctx, p.bucket)
	if err != nil {
		if errors.Is(err, jetstream.ErrBucketNotFound) || errors.Is(err, jetstream.ErrStreamNotFound) {
			return fmt.Errorf("%w: bucket %s does not exist yet", ErrKeyNotFound, p.bucket)
		}
		return fmt.Errorf("%w: open %s: %w", ErrUnavailable, p.bucket, err)
	}

	p.mu.Lock()
	p.accessKeys = kv
	p.mu.Unlock()
	p.startWatcher(kv)
	return nil
}

func (p *Provider) ensureSessions(ctx context.Context) error {
	p.mu.RLock()
	ready := p.sessions != nil
	p.mu.RUnlock()
	if ready {
		return nil
	}

	kv, err := p.js.KeyValue(ctx, sessionCredentialsBucket)
	if err != nil {
		if errors.Is(err, jetstream.ErrBucketNotFound) || errors.Is(err, jetstream.ErrStreamNotFound) {
			return fmt.Errorf("%w: session bucket does not exist yet", ErrKeyNotFound)
		}
		return fmt.Errorf("%w: open session bucket: %w", ErrUnavailable, err)
	}

	p.mu.Lock()
	p.sessions = kv
	p.mu.Unlock()
	return nil
}

// startWatcher drops cached credentials as the bucket changes, so revoking a
// key takes effect immediately rather than after the TTL.
func (p *Provider) startWatcher(kv jetstream.KeyValue) {
	p.watcherOnce.Do(func() {
		watcher, err := kv.WatchAll(context.Background())
		if err != nil {
			p.log.Error("IAM key watcher unavailable; credential changes take effect "+
				"only after the cache expires", "error", err, "cache_ttl_ms", cacheTTL.Milliseconds())
			return
		}
		p.watcher = watcher
		go p.watchChanges(watcher)
	})
}

func (p *Provider) watchChanges(watcher jetstream.KeyWatcher) {
	for {
		select {
		case <-p.watcherStop:
			return
		case update, ok := <-watcher.Updates():
			if !ok {
				return
			}
			// A nil update marks the end of the initial replay, not a change.
			if update == nil {
				continue
			}
			p.mu.Lock()
			delete(p.cache, update.Key())
			p.mu.Unlock()
		}
	}
}

// Close stops the watcher. The NATS connection belongs to the caller.
func (p *Provider) Close() {
	p.closedSignal.Do(func() {
		close(p.watcherStop)
		if p.watcher != nil {
			if err := p.watcher.Stop(); err != nil {
				p.log.Warn("stopping IAM key watcher", "error", err)
			}
		}
	})
}
