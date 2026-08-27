// Command goannad ingests per-VM metrics from NATS into a local TSDB and
// serves them over a SigV4-authenticated CloudWatch API. The API is opt-in:
// without --api-addr the daemon only ingests.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"golang.org/x/sync/errgroup"

	"github.com/mulgadc/goanna/internal/health"
	"github.com/mulgadc/goanna/internal/ingest"
	"github.com/mulgadc/goanna/internal/store"
)

// Version is set at build time.
var Version = "dev"

type config struct {
	natsURL    string
	natsToken  string
	dataDir    string
	retention  time.Duration
	logLevel   string
	streamName string
	healthAddr string

	// CloudWatch API. apiAddr is empty when the API is not being served,
	// which is how a node runs ingest-only.
	apiAddr          string
	tlsCert          string
	tlsKey           string
	masterKeyPath    string
	accessKeysBucket string
	region           string
}

func main() {
	// Flag parsing and signal wiring live here rather than in run, so run
	// takes both as arguments and a test can drive it with neither.
	cfg, err := parseFlags(flag.NewFlagSet("goannad", flag.ExitOnError), os.Args[1:])
	if err != nil {
		slog.Error("goannad exited", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg); err != nil {
		slog.Error("goannad exited", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg config) error {
	logger, err := newLogger(cfg.logLevel)
	if err != nil {
		return err
	}
	slog.SetDefault(logger)
	logger.Info("starting goannad", "version", Version, "data_dir", cfg.dataDir)

	metrics, err := store.Open(store.Options{
		Dir:         cfg.dataDir,
		RetentionMS: cfg.retention.Milliseconds(),
		Logger:      logger,
	})
	if err != nil {
		return err
	}
	defer func() {
		if err := metrics.Close(); err != nil {
			logger.Error("closing store", "error", err)
		}
	}()

	nc, err := connectNATS(cfg, logger)
	if err != nil {
		return err
	}
	defer nc.Close()

	in, err := ingest.New(nc, metrics, ingest.Config{
		Stream: cfg.streamName,
		Logger: logger,
	})
	if err != nil {
		return err
	}

	probes := health.New(health.Options{
		Version: Version,
		Checks: map[string]health.Check{
			"nats": func() error {
				if !nc.IsConnected() {
					return fmt.Errorf("not connected to %s", cfg.natsURL)
				}
				return nil
			},
			"store": metrics.Ready,
		},
		Details: map[string]health.Detail{
			"ingest": func() any { return in.Stats() },
			"store":  func() any { return metrics.Stats() },
		},
		Logger: logger,
	})

	// Readiness deliberately excludes metric freshness. A node with no
	// running guests produces no batches, and that is healthy.
	group, gctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		logger.Info("serving health", "addr", cfg.healthAddr)
		return probes.Serve(gctx, cfg.healthAddr)
	})
	group.Go(func() error {
		logger.Info("consuming", "subject", ingest.DefaultSubject)
		return in.Run(gctx)
	})

	if cfg.apiAddr != "" {
		handler, provider, err := buildAPI(cfg, metrics, nc, logger)
		if err != nil {
			return err
		}
		defer provider.Close()
		group.Go(func() error {
			logger.Info("serving cloudwatch api", "addr", cfg.apiAddr, "region", cfg.region)
			return serveTLS(gctx, cfg, handler)
		})
	}

	if err := group.Wait(); err != nil {
		return err
	}
	logger.Info("shutting down")
	return nil
}

// parseFlags reads configuration from args, falling back to the environment
// and then to the built-in defaults. The flag set is a parameter so a test
// can parse without the test binary's own flags being registered on it.
func parseFlags(fs *flag.FlagSet, args []string) (config, error) {
	var cfg config
	fs.StringVar(&cfg.natsURL, "nats-url", env("GOANNA_NATS_URL", nats.DefaultURL), "NATS server URL")
	fs.StringVar(&cfg.natsToken, "nats-token", env("GOANNA_NATS_TOKEN", ""), "NATS auth token")
	fs.StringVar(&cfg.dataDir, "data-dir", env("GOANNA_DATA_DIR", "/var/lib/goanna/tsdb"), "TSDB directory")
	fs.StringVar(&cfg.logLevel, "log-level", env("GOANNA_LOG_LEVEL", "info"), "debug, info, warn or error")
	fs.StringVar(&cfg.healthAddr, "health-addr", env("GOANNA_HEALTH_ADDR", "127.0.0.1:8445"), "liveness, readiness and status listener")
	fs.StringVar(&cfg.streamName, "stream", env("GOANNA_STREAM", ingest.DefaultStream), "JetStream stream name")
	fs.StringVar(&cfg.apiAddr, "api-addr", env("GOANNA_API_ADDR", ""), "CloudWatch API listener; empty serves ingest only")
	fs.StringVar(&cfg.tlsCert, "tls-cert", env("GOANNA_TLS_CERT", ""), "PEM certificate for the API listener")
	fs.StringVar(&cfg.tlsKey, "tls-key", env("GOANNA_TLS_KEY", ""), "PEM private key for the API listener")
	fs.StringVar(&cfg.masterKeyPath, "master-key", env("GOANNA_MASTER_KEY", "/etc/spinifex/master.key"), "shared IAM master key")
	fs.StringVar(&cfg.accessKeysBucket, "access-keys-bucket", env("GOANNA_ACCESS_KEYS_BUCKET", ""), "IAM access-keys KV bucket")
	fs.StringVar(&cfg.region, "region", env("GOANNA_REGION", "ap-southeast-2"), "region clients must sign with")
	fs.DurationVar(&cfg.retention, "retention", 15*24*time.Hour, "how long to keep samples locally")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	return cfg, nil
}

// connectNATS dials with reconnection left on. A metric producer that
// outlives a broker restart is the normal case, and JetStream replays what
// was missed once the link is back.
func connectNATS(cfg config, logger *slog.Logger) (*nats.Conn, error) {
	opts := []nats.Option{
		nats.Name("goannad"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2 * time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			logger.Warn("nats disconnected", "error", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			logger.Info("nats reconnected", "url", nc.ConnectedUrl())
		}),
	}
	if cfg.natsToken != "" {
		opts = append(opts, nats.Token(cfg.natsToken))
	}

	nc, err := nats.Connect(cfg.natsURL, opts...)
	if err != nil {
		return nil, fmt.Errorf("connect to nats at %s: %w", cfg.natsURL, err)
	}
	logger.Info("connected to nats", "url", nc.ConnectedUrl())
	return nc, nil
}

func newLogger(level string) (*slog.Logger, error) {
	var l slog.Level
	switch strings.ToLower(level) {
	case "debug":
		l = slog.LevelDebug
	case "info":
		l = slog.LevelInfo
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		return nil, errors.New("log level must be debug, info, warn or error")
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: l})), nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
