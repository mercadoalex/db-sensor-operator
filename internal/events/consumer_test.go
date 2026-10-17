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

package events

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	kafka "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sensoriov1 "github.com/mercadoalex/db-sensor-operator/api/v1"
)

const insertFixture = `{"payload":{"op":"c","ts_ms":1700000000000,"source":{"db":"pg","table":"orders"}}}`

// Common fixture literals shared across the event-package tests.
const (
	tableOrders = "orders"
	dbOrders    = "orders_db"
)

func TestHTTPConsumerHandler(t *testing.T) {
	var got []CDCEvent
	h := (&HTTPConsumer{}).Handler(func(ev CDCEvent) { got = append(got, ev) })
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Post(srv.URL, "application/json", strings.NewReader(insertFixture))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	require.Len(t, got, 1)
	assert.Equal(t, CDCEvent{Op: "c", DB: "pg", Table: tableOrders, TsMs: 1700000000000}, got[0])
}

func TestHTTPConsumerHandlerMalformed(t *testing.T) {
	called := false
	h := (&HTTPConsumer{}).Handler(func(CDCEvent) { called = true })
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Post(srv.URL, "application/json", strings.NewReader(`{bad`))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.False(t, called)
}

func TestHTTPConsumerHandlerRejectsGet(t *testing.T) {
	h := (&HTTPConsumer{}).Handler(func(CDCEvent) {})
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
}

// mockReader is a fake kafkaReader that yields pre-seeded messages then blocks on
// ctx cancellation, so KafkaConsumer.Start can be tested without a broker.
type mockReader struct {
	mu       sync.Mutex
	msgs     []kafka.Message
	idx      int
	closed   bool
	errAfter error
}

func (m *mockReader) ReadMessage(ctx context.Context) (kafka.Message, error) {
	m.mu.Lock()
	if m.idx < len(m.msgs) {
		msg := m.msgs[m.idx]
		m.idx++
		m.mu.Unlock()
		return msg, nil
	}
	m.mu.Unlock()
	if m.errAfter != nil {
		return kafka.Message{}, m.errAfter
	}
	// No more fixtures: wait for cancellation to mimic a quiet topic.
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (m *mockReader) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func TestKafkaConsumerStart(t *testing.T) {
	reader := &mockReader{msgs: []kafka.Message{
		{Value: []byte(insertFixture)},
		{Value: []byte(`{bad json`)}, // dropped, must not stop the loop
		{Value: []byte(`{"payload":{"op":"d","ts_ms":1700000001000,"source":{"db":"pg","table":"orders"}}}`)},
	}}

	var mu sync.Mutex
	var got []CDCEvent
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		done <- (&KafkaConsumer{Reader: reader}).Start(ctx, func(ev CDCEvent) {
			mu.Lock()
			got = append(got, ev)
			mu.Unlock()
		})
	}()

	// Give the loop time to drain fixtures, then cancel to end it.
	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) == 2
	}, time.Second, 5*time.Millisecond)
	cancel()

	assert.NoError(t, <-done)
	assert.True(t, reader.closed, "reader should be closed on Start return")

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, got, 2)
	assert.Equal(t, "c", got[0].Op)
	assert.Equal(t, "d", got[1].Op)
}

func TestNewConsumerFactory(t *testing.T) {
	t.Run("empty defaults to http", func(t *testing.T) {
		c, err := NewConsumer(sensoriov1.TransportConfig{})
		require.NoError(t, err)
		assert.IsType(t, &HTTPConsumer{}, c)
	})
	t.Run("http", func(t *testing.T) {
		c, err := NewConsumer(sensoriov1.TransportConfig{Type: "http"})
		require.NoError(t, err)
		assert.IsType(t, &HTTPConsumer{}, c)
	})
	t.Run("kafka", func(t *testing.T) {
		c, err := NewConsumer(sensoriov1.TransportConfig{Type: "kafka", Kafka: &sensoriov1.KafkaTransport{
			Brokers: []string{"localhost:9092"}, Topic: "cdc", GroupID: "g",
		}})
		require.NoError(t, err)
		kc, ok := c.(*KafkaConsumer)
		require.True(t, ok)
		// Close the underlying reader so the test leaves no goroutine/fd behind.
		if kc.Reader != nil {
			_ = kc.Reader.Close()
		}
	})
	t.Run("unknown errors", func(t *testing.T) {
		c, err := NewConsumer(sensoriov1.TransportConfig{Type: "rabbitmq"})
		assert.Nil(t, c)
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrUnsupportedTransport))
	})
}
