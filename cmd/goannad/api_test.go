package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"encoding/xml"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/mulgadc/bluebottle/pkg/masterkey"
	"github.com/mulgadc/goanna/internal/store"
)

// writeTestTLS generates a self-signed certificate for 127.0.0.1 and returns
// the cert and key paths.
func writeTestTLS(t *testing.T) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "goanna-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}

	dir := t.TempDir()
	certPath := filepath.Join(dir, "server.pem")
	keyPath := filepath.Join(dir, "server.key")
	writePEM(t, certPath, "CERTIFICATE", der)
	writePEM(t, keyPath, "EC PRIVATE KEY", keyDER)
	return certPath, keyPath
}

func writePEM(t *testing.T, path, blockType string, der []byte) {
	t.Helper()
	body := pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der})
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// writeTestMasterKey writes a random key at the group-readable mode the
// deployed file uses.
func writeTestMasterKey(t *testing.T) string {
	t.Helper()
	raw := make([]byte, masterkey.MasterKeySize)
	if _, err := rand.Read(raw); err != nil {
		t.Fatalf("random key: %v", err)
	}
	path := filepath.Join(t.TempDir(), "master.key")
	if err := os.WriteFile(path, raw, 0o640); err != nil {
		t.Fatalf("write master key: %v", err)
	}
	return path
}

// Serving the API unconfigured would either listen in plaintext or resolve no
// credential at all, so each missing input has to fail the build.
func TestBuildAPIRequiresTLSRegionAndMasterKey(t *testing.T) {
	cert, key := writeTestTLS(t)
	master := writeTestMasterKey(t)
	nc, err := nats.Connect(startNATS(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	tests := []struct {
		name string
		cfg  config
	}{
		{"no certificate", config{tlsKey: key, region: "ap-southeast-2", masterKeyPath: master}},
		{"no private key", config{tlsCert: cert, region: "ap-southeast-2", masterKeyPath: master}},
		{"no region", config{tlsCert: cert, tlsKey: key, masterKeyPath: master}},
		{"no master key", config{tlsCert: cert, tlsKey: key, region: "ap-southeast-2"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, provider, err := buildAPI(tt.cfg, nil, nc, slog.Default())
			if err == nil {
				provider.Close()
				t.Fatalf("want an error, got handler %v", handler)
			}
			if provider != nil {
				t.Error("a failed build still returned a provider to close")
			}
		})
	}
}

// The happy path has to produce a handler, and the provider has to be the
// caller's to close.
func TestBuildAPIReturnsAClosableProvider(t *testing.T) {
	cert, key := writeTestTLS(t)
	nc, err := nats.Connect(startNATS(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	metrics, err := store.Open(store.Options{Dir: filepath.Join(t.TempDir(), "tsdb")})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := metrics.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	}()

	handler, provider, err := buildAPI(config{
		tlsCert:       cert,
		tlsKey:        key,
		region:        "ap-southeast-2",
		masterKeyPath: writeTestMasterKey(t),
	}, metrics, nc, slog.Default())
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer provider.Close()
	if handler == nil {
		t.Error("no handler")
	}
}

// The whole chain has to come up: TLS listener, SigV4 gate, CloudWatch
// handler. An unsigned request proves all three are wired without needing an
// IAM record to exist.
func TestRunServesTheAPIBehindTheSigV4Gate(t *testing.T) {
	certPath, keyPath := writeTestTLS(t)
	cfg := config{
		natsURL:       startNATS(t),
		dataDir:       filepath.Join(t.TempDir(), "tsdb"),
		retention:     24 * time.Hour,
		logLevel:      "error",
		healthAddr:    freeAddr(t),
		apiAddr:       freeAddr(t),
		tlsCert:       certPath,
		tlsKey:        keyPath,
		masterKeyPath: writeTestMasterKey(t),
		region:        "ap-southeast-2",
	}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("run: %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Error("run did not return after cancel")
		}
	}()

	// The certificate is the test's own, generated above.
	client := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12},
	}}

	var status int
	var body string
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		//nolint:noctx // short-lived probe in a test
		resp, err := client.Post("https://"+cfg.apiAddr+"/", "application/x-www-form-urlencoded",
			strings.NewReader("Action=ListMetrics"))
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		raw, readErr := io.ReadAll(resp.Body)
		if err := resp.Body.Close(); err != nil {
			t.Fatalf("close body: %v", err)
		}
		if readErr != nil {
			t.Fatalf("read body: %v", readErr)
		}
		status, body = resp.StatusCode, string(raw)
		break
	}

	if status != http.StatusForbidden {
		t.Fatalf("status = %d, body = %s", status, body)
	}
	var errResp struct {
		Error struct {
			Code string `xml:"Code"`
		} `xml:"Error"`
	}
	if err := xml.Unmarshal([]byte(body), &errResp); err != nil {
		t.Fatalf("unmarshal %q: %v", body, err)
	}
	if errResp.Error.Code != "MissingAuthenticationToken" {
		t.Errorf("code = %s, want MissingAuthenticationToken", errResp.Error.Code)
	}
}

// An unusable certificate must fail the daemon rather than leave the API
// silently unserved.
func TestRunFailsOnAnUnusableCertificate(t *testing.T) {
	cfg := config{
		natsURL:       startNATS(t),
		dataDir:       filepath.Join(t.TempDir(), "tsdb"),
		retention:     24 * time.Hour,
		logLevel:      "error",
		healthAddr:    freeAddr(t),
		apiAddr:       freeAddr(t),
		tlsCert:       filepath.Join(t.TempDir(), "absent.pem"),
		tlsKey:        filepath.Join(t.TempDir(), "absent.key"),
		masterKeyPath: writeTestMasterKey(t),
		region:        "ap-southeast-2",
	}

	if err := run(t.Context(), cfg); err == nil {
		t.Fatal("want an error for a certificate that cannot be loaded")
	}
}

func TestParseFlagsAPIDefaults(t *testing.T) {
	cfg, err := parseFlags(testFlagSet(), nil)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	// The API is opt-in, so a node that only ingests needs no new config.
	if cfg.apiAddr != "" {
		t.Errorf("api addr = %q, want empty", cfg.apiAddr)
	}
	if cfg.masterKeyPath != "/etc/spinifex/master.key" {
		t.Errorf("master key = %q", cfg.masterKeyPath)
	}
	if cfg.region != "ap-southeast-2" {
		t.Errorf("region = %q", cfg.region)
	}
}
