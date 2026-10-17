/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// This file (Task 7, transport-agnostic parts) is the CDC event source. A
// Consumer receives raw Debezium events from a transport, parses each one, and
// invokes a handler callback with the resulting CDCEvent.
//
// ADR-012 (HTTP default, Kafka optional) and its refinement: the Consumer is
// decoupled from the metrics Exporter and the LagTracker. Rather than taking a
// *Exporter, Start and NewConsumer take a `handler func(CDCEvent)`; the Pipeline
// (pipeline.go) builds that handler so it can fan out to both the Exporter and
// the LagTracker. This keeps the transport layer dependency-free and trivially
// mockable.
//
// The HTTPConsumer is the default because the HTTP sink needs no broker, which
// is why it is the dev/test loop (docs/TESTING.md). The KafkaConsumer sits
// behind the minimal kafkaReader interface so it is unit-tested with a mock,
// never a real broker.
package events

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	kafka "github.com/segmentio/kafka-go"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	sensoriov1 "github.com/mercadoalex/db-sensor-operator/api/v1"
)

// defaultHTTPAddr is the HTTPConsumer's standalone listen address. It is :8080
// (NOT :8081) so the constant never shadows the manager's health-probe port
// (DECISION C). Today CDC ingestion is mounted as POST /cdc on the operator HTTP
// server (cmd/main.go), so this standalone address is used only if a caller runs
// HTTPConsumer.Start directly (e.g. a test or an alternate wiring).
const defaultHTTPAddr = ":8080"

// Consumer receives CDC events from a transport and delivers each parsed event
// to handler. Start blocks until ctx is cancelled (or the transport ends) and
// returns any non-shutdown error.
type Consumer interface {
	Start(ctx context.Context, handler func(CDCEvent)) error
}

// NewConsumer returns the Consumer for the configured transport: "" or "http"
// yields an HTTPConsumer (the default), "kafka" yields a KafkaConsumer. Any
// other value is a wrapped ErrUnsupportedTransport.
//
// The handler is supplied to the returned Consumer's Start method; it is invoked
// for every successfully parsed event.
//
// cfg is the api/v1 TransportConfig (ADR-012). The HTTPConsumer listens on
// defaultHTTPAddr; the operator-wide listen address is applied by cmd/main.go,
// since the transport addr is operator config rather than a per-CR field.
func NewConsumer(cfg sensoriov1.TransportConfig) (Consumer, error) {
	switch cfg.Type {
	case "", "http":
		return &HTTPConsumer{Addr: defaultHTTPAddr}, nil
	case "kafka":
		var kcfg sensoriov1.KafkaTransport
		if cfg.Kafka != nil {
			kcfg = *cfg.Kafka
		}
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers: kcfg.Brokers,
			Topic:   kcfg.Topic,
			GroupID: kcfg.GroupID,
		})
		return &KafkaConsumer{Reader: reader}, nil
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedTransport, cfg.Type)
	}
}

// ErrUnsupportedTransport is returned by NewConsumer for an unknown
// TransportConfig.Type.
var ErrUnsupportedTransport = errors.New("unsupported transport type")

// HTTPConsumer receives Debezium events as HTTP POST bodies. It is the default
// transport (ADR-012): no broker required.
type HTTPConsumer struct {
	// Addr is the listen address used by Start (e.g. ":8081").
	Addr string
}

// Handler returns an http.Handler that parses each POST body into a CDCEvent and
// calls handler on success. It is mountable into an external ServeMux by Task 15
// without running HTTPConsumer's own server. A malformed body is answered with
// 400 and does not invoke handler (and never panics).
func (h *HTTPConsumer) Handler(handler func(CDCEvent)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log := logf.FromContext(r.Context())
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "cannot read body", http.StatusBadRequest)
			return
		}
		ev, err := Parse(body)
		if err != nil {
			log.Error(err, "Dropped malformed CDC event")
			http.Error(w, "malformed event", http.StatusBadRequest)
			return
		}
		if handler != nil {
			handler(ev)
		}
		w.WriteHeader(http.StatusNoContent)
	})
}

// Start runs an HTTP server on h.Addr serving Handler(handler) and shuts it down
// gracefully when ctx is cancelled.
func (h *HTTPConsumer) Start(ctx context.Context, handler func(CDCEvent)) error {
	log := logf.FromContext(ctx)

	mux := http.NewServeMux()
	mux.Handle("/", h.Handler(handler))
	srv := &http.Server{Addr: h.Addr, Handler: mux}

	errCh := make(chan error, 1)
	go func() {
		log.Info("Starting HTTP CDC consumer", "addr", h.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("HTTP consumer server: %w", err)
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutting down HTTP consumer: %w", err)
		}
		return nil
	case err := <-errCh:
		return err
	}
}

// kafkaReader is the minimal subset of *kafka.Reader the KafkaConsumer needs. It
// exists so the consumer loop can be unit-tested with a mock reader (no broker).
// *kafka.Reader satisfies this interface.
type kafkaReader interface {
	ReadMessage(ctx context.Context) (kafka.Message, error)
	Close() error
}

// KafkaConsumer receives Debezium events from a Kafka topic. It is the optional
// transport (ADR-012) and depends only on the kafkaReader interface.
type KafkaConsumer struct {
	Reader kafkaReader
}

// Start loops reading messages, parsing each into a CDCEvent and calling
// handler. It returns nil on context cancellation or EOF, and wraps any other
// read error. A malformed message is logged and skipped so one poison record
// cannot stop the loop.
func (k *KafkaConsumer) Start(ctx context.Context, handler func(CDCEvent)) error {
	log := logf.FromContext(ctx)
	defer func() { _ = k.Reader.Close() }()

	for {
		msg, err := k.Reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("reading Kafka message: %w", err)
		}
		ev, parseErr := Parse(msg.Value)
		if parseErr != nil {
			log.Error(parseErr, "Dropped malformed CDC event")
			continue
		}
		if handler != nil {
			handler(ev)
		}
	}
}
