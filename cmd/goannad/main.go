// Command goannad ingests per-VM metrics from NATS into a local TSDB.
//
// Phase 1: there is no CloudWatch API yet. This exists to get tenant
// metrics durable and queryable.
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

	logger.Info("consuming", "subject", ingest.DefaultSubject)
	if err := in.Run(ctx); err != nil {
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
	fs.StringVar(&cfg.streamName, "stream", env("GOANNA_STREAM", ingest.DefaultStream), "JetStream stream name")
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
