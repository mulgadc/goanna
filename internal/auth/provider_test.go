package auth

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mulgadc/bluebottle/pkg/masterkey"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// startNATS runs an in-process NATS server with JetStream enabled.
func startNATS(t *testing.T) *nats.Conn {
	t.Helper()
	srv, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, JetStream: true,
		StoreDir: t.TempDir(), NoLog: true, NoSigs: true,
	})
	if err != nil {
		t.Fatalf("nats server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats server did not start")
	}
	t.Cleanup(srv.Shutdown)

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

// writeMasterKey writes a random 32-byte key at the group-readable mode the
// deployed file uses.
func writeMasterKey(t *testing.T) (string, *masterkey.Key) {
	t.Helper()
	raw := make([]byte, masterkey.MasterKeySize)
	if _, err := rand.Read(raw); err != nil {
		t.Fatalf("random key: %v", err)
	}
	path := filepath.Join(t.TempDir(), "master.key")
	if err := os.WriteFile(path, raw, 0o640); err != nil {
		t.Fatalf("write key: %v", err)
	}
	key, err := masterkey.New(raw)
	if err != nil {
		t.Fatalf("build key: %v", err)
	}
	return path, key
}

func createBucket(t *testing.T, nc *nats.Conn, name string) jetstream.KeyValue {
	t.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	kv, err := js.CreateKeyValue(t.Context(), jetstream.KeyValueConfig{Bucket: name})
	if err != nil {
		t.Fatalf("create bucket %s: %v", name, err)
	}
	return kv
}

func putJSON(t *testing.T, kv jetstream.KeyValue, key string, value any) {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, err := kv.Put(t.Context(), key, body); err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
}

func newTestProvider(t *testing.T, nc *nats.Conn, keyPath string) *Provider {
	t.Helper()
	p, err := NewProvider(nc, Config{MasterKeyPath: keyPath})
	if err != nil {
		t.Fatalf("new provider: %v", err)
	}
	t.Cleanup(p.Close)
	return p
}

func TestProviderResolvesALongLivedKey(t *testing.T) {
	nc := startNATS(t)
	keyPath, key := writeMasterKey(t)
	kv := createBucket(t, nc, defaultAccessKeysBucket)

	secret, err := key.EncryptBase64("the-secret")
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	putJSON(t, kv, "AKIATEST", accessKeyRecord{
		SecretAccessKey: secret, UserName: "alice",
		AccountID: "111122223333", Status: accessKeyStatusActive,
	})

	p := newTestProvider(t, nc, keyPath)
	cred, err := p.Lookup(t.Context(), "AKIATEST")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if cred.SecretAccessKey != "the-secret" || cred.AccountID != "111122223333" || cred.UserName != "alice" {
		t.Errorf("credential = %+v", cred)
	}
}

func TestProviderRejectsUnusableAccessKeys(t *testing.T) {
	nc := startNATS(t)
	keyPath, key := writeMasterKey(t)
	kv := createBucket(t, nc, defaultAccessKeysBucket)

	good, err := key.EncryptBase64("s")
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	putJSON(t, kv, "AKIAINACTIVE", accessKeyRecord{
		SecretAccessKey: good, AccountID: "111122223333", Status: "Inactive",
	})
	// An empty account would scope every query to the series with no account
	// label at all.
	putJSON(t, kv, "AKIANOACCOUNT", accessKeyRecord{
		SecretAccessKey: good, Status: accessKeyStatusActive,
	})
	putJSON(t, kv, "AKIABADSECRET", accessKeyRecord{
		SecretAccessKey: "not-encrypted-with-this-key",
		AccountID:       "111122223333", Status: accessKeyStatusActive,
	})

	p := newTestProvider(t, nc, keyPath)
	for _, id := range []string{"AKIAINACTIVE", "AKIANOACCOUNT", "AKIABADSECRET", "AKIAMISSING"} {
		t.Run(id, func(t *testing.T) {
			if _, err := p.Lookup(t.Context(), id); !errors.Is(err, ErrKeyNotFound) {
				t.Errorf("error = %v, want ErrKeyNotFound", err)
			}
		})
	}
}

// A prefix neither store owns must not be dispatched to either.
func TestProviderRejectsAnUnknownPrefix(t *testing.T) {
	nc := startNATS(t)
	keyPath, _ := writeMasterKey(t)
	createBucket(t, nc, defaultAccessKeysBucket)

	p := newTestProvider(t, nc, keyPath)
	if _, err := p.Lookup(t.Context(), "ROOTKEY000000000"); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("error = %v, want ErrKeyNotFound", err)
	}
}

func TestProviderResolvesASessionCredential(t *testing.T) {
	nc := startNATS(t)
	keyPath, key := writeMasterKey(t)
	kv := createBucket(t, nc, sessionCredentialsBucket)

	secret, err := key.EncryptBase64("session-secret")
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	putJSON(t, kv, "ASIALIVE", sessionRecord{
		SecretEncrypted: secret, AccountID: "111122223333",
		SessionName: "i-0abc", ExpiresAt: time.Now().UTC().Add(time.Hour),
	})
	// Expired records stay readable until the STS janitor reaps them.
	putJSON(t, kv, "ASIAEXPIRED", sessionRecord{
		SecretEncrypted: secret, AccountID: "111122223333",
		SessionName: "i-0abc", ExpiresAt: time.Now().UTC().Add(-time.Minute),
	})

	p := newTestProvider(t, nc, keyPath)
	cred, err := p.Lookup(t.Context(), "ASIALIVE")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if cred.SecretAccessKey != "session-secret" || cred.UserName != "i-0abc" {
		t.Errorf("credential = %+v", cred)
	}

	if _, err := p.Lookup(t.Context(), "ASIAEXPIRED"); !errors.Is(err, ErrExpired) {
		t.Errorf("error = %v, want ErrExpired", err)
	}
}

// goannad may start before the spinifex daemon has created the buckets. That
// must not be fatal, and must not look like an outage either.
func TestProviderToleratesMissingBuckets(t *testing.T) {
	nc := startNATS(t)
	keyPath, _ := writeMasterKey(t)

	p := newTestProvider(t, nc, keyPath)
	if _, err := p.Lookup(t.Context(), "AKIATEST"); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("error = %v, want ErrKeyNotFound", err)
	}
	if _, err := p.Lookup(t.Context(), "ASIATEST"); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("error = %v, want ErrKeyNotFound", err)
	}
}

// Revoking a key has to take effect without waiting out the cache TTL.
func TestProviderWatcherInvalidatesTheCache(t *testing.T) {
	nc := startNATS(t)
	keyPath, key := writeMasterKey(t)
	kv := createBucket(t, nc, defaultAccessKeysBucket)

	secret, err := key.EncryptBase64("s")
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	active := accessKeyRecord{
		SecretAccessKey: secret, AccountID: "111122223333", Status: accessKeyStatusActive,
	}
	putJSON(t, kv, "AKIAREVOKE", active)

	p := newTestProvider(t, nc, keyPath)
	if _, err := p.Lookup(t.Context(), "AKIAREVOKE"); err != nil {
		t.Fatalf("first lookup: %v", err)
	}

	revoked := active
	revoked.Status = "Inactive"
	putJSON(t, kv, "AKIAREVOKE", revoked)

	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := p.Lookup(t.Context(), "AKIAREVOKE")
		if errors.Is(err, ErrKeyNotFound) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("a revoked key still resolved after the watcher should have "+
				"invalidated it: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestNewProviderRequiresItsInputs(t *testing.T) {
	nc := startNATS(t)
	keyPath, _ := writeMasterKey(t)

	if _, err := NewProvider(nil, Config{MasterKeyPath: keyPath}); err == nil {
		t.Error("want an error without a NATS connection")
	}
	if _, err := NewProvider(nc, Config{}); err == nil {
		t.Error("want an error without a master key path")
	}
	if _, err := NewProvider(nc, Config{MasterKeyPath: filepath.Join(t.TempDir(), "absent")}); err == nil {
		t.Error("want an error for an unreadable master key")
	}
}

// The master key is group-readable by design, but world-readable is a breach.
func TestNewProviderRejectsALooseMasterKey(t *testing.T) {
	nc := startNATS(t)
	path := filepath.Join(t.TempDir(), "master.key")
	raw := make([]byte, masterkey.MasterKeySize)
	if _, err := rand.Read(raw); err != nil {
		t.Fatalf("random key: %v", err)
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("write key: %v", err)
	}

	if _, err := NewProvider(nc, Config{MasterKeyPath: path}); err == nil {
		t.Error("want an error for a world-readable master key")
	}
}
