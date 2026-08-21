package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/mulgadc/goanna/internal/auth"
	"github.com/mulgadc/goanna/internal/cloudwatch"
	"github.com/mulgadc/goanna/internal/store"
)

// apiReadHeaderTimeout bounds how long an unauthenticated connection may hold
// a slot before it has sent anything worth reading.
const apiReadHeaderTimeout = 10 * time.Second

// buildAPI assembles the CloudWatch handler behind the SigV4 gate. The
// returned provider is the caller's to close.
func buildAPI(cfg config, metrics *store.Store, nc *nats.Conn,
	logger *slog.Logger) (http.Handler, *auth.Provider, error) {
	if cfg.tlsCert == "" || cfg.tlsKey == "" {
		return nil, nil, errors.New("serving the API requires --tls-cert and --tls-key")
	}
	if cfg.region == "" {
		return nil, nil, errors.New("serving the API requires --region")
	}

	provider, err := auth.NewProvider(nc, auth.Config{
		MasterKeyPath:    cfg.masterKeyPath,
		AccessKeysBucket: cfg.accessKeysBucket,
		Logger:           logger,
	})
	if err != nil {
		return nil, nil, err
	}

	api, err := cloudwatch.New(cloudwatch.Options{Store: metrics, Logger: logger})
	if err != nil {
		provider.Close()
		return nil, nil, err
	}

	gate := auth.Middleware(auth.MiddlewareOptions{
		Provider: provider,
		Region:   cfg.region,
		Logger:   logger,
	})
	return gate(api), provider, nil
}

// serveTLS runs the API until ctx is done. TLS 1.2 is the floor: the AWS SDKs
// all negotiate 1.2 or better, so nothing older needs to be accepted.
func serveTLS(ctx context.Context, cfg config, handler http.Handler) error {
	srv := &http.Server{
		Addr:              cfg.apiAddr,
		Handler:           handler,
		ReadHeaderTimeout: apiReadHeaderTimeout,
		TLSConfig:         &tls.Config{MinVersion: tls.VersionTLS12},
	}

	errc := make(chan error, 1)
	go func() {
		err := srv.ListenAndServeTLS(cfg.tlsCert, cfg.tlsKey)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- fmt.Errorf("cloudwatch api: listen on %s: %w", cfg.apiAddr, err)
			return
		}
		errc <- nil
	}()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdown); err != nil {
			return fmt.Errorf("cloudwatch api: shutdown: %w", err)
		}
		return nil
	}
}
