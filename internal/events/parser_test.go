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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want CDCEvent
	}{
		{
			name: "insert in payload envelope",
			raw: `{"payload":{"op":"c","ts_ms":1700000000000,
				"source":{"db":"orders_db","table":"orders"}}}`,
			want: CDCEvent{Op: "c", DB: dbOrders, Table: tableOrders, TsMs: 1700000000000},
		},
		{
			name: "update",
			raw: `{"payload":{"op":"u","ts_ms":1700000001000,
				"source":{"db":"orders_db","table":"customers"}}}`,
			want: CDCEvent{Op: "u", DB: dbOrders, Table: "customers", TsMs: 1700000001000},
		},
		{
			name: "delete",
			raw: `{"payload":{"op":"d","ts_ms":1700000002000,
				"source":{"db":"orders_db","table":"orders"}}}`,
			want: CDCEvent{Op: "d", DB: dbOrders, Table: tableOrders, TsMs: 1700000002000},
		},
		{
			name: "truncate",
			raw: `{"payload":{"op":"t","ts_ms":1700000002500,
				"source":{"db":"orders_db","table":"orders"}}}`,
			want: CDCEvent{Op: "t", DB: dbOrders, Table: tableOrders, TsMs: 1700000002500},
		},
		{
			name: "top-level op without payload wrapper",
			raw:  `{"op":"c","ts_ms":1700000003000,"source":{"db":"inv","table":"stock"}}`,
			want: CDCEvent{Op: "c", DB: "inv", Table: "stock", TsMs: 1700000003000},
		},
		{
			name: "ddl via payload.ddl",
			raw: `{"payload":{"ddl":"ALTER TABLE orders ADD COLUMN note text","ts_ms":1700000004000,
				"source":{"db":"orders_db","table":"orders"}}}`,
			want: CDCEvent{Op: "ddl", DB: dbOrders, Table: tableOrders, TsMs: 1700000004000},
		},
		{
			name: "ddl via schema-history historyRecord marker",
			raw: `{"payload":{"historyRecord":"{}","ts_ms":1700000005000,
				"source":{"db":"orders_db","table":"orders"}}}`,
			want: CDCEvent{Op: "ddl", DB: dbOrders, Table: tableOrders, TsMs: 1700000005000},
		},
		{
			name: "mongodb collection maps to table",
			raw: `{"payload":{"op":"c","ts_ms":1700000006000,
				"source":{"name":"mongo_rs","collection":"sessions"}}}`,
			want: CDCEvent{Op: "c", DB: "mongo_rs", Table: "sessions", TsMs: 1700000006000},
		},
		{
			name: "db falls back to source.name",
			raw: `{"payload":{"op":"u","ts_ms":1700000007000,
				"source":{"name":"srv1","table":"t1"}}}`,
			want: CDCEvent{Op: "u", DB: "srv1", Table: "t1", TsMs: 1700000007000},
		},
		{
			name: "ts_ms falls back to source.ts_ms",
			raw: `{"payload":{"op":"c",
				"source":{"db":"d","table":"t","ts_ms":1700000008000}}}`,
			want: CDCEvent{Op: "c", DB: "d", Table: "t", TsMs: 1700000008000},
		},
		{
			name: "snapshot read op=r is a snapshot event",
			raw: `{"payload":{"op":"r","ts_ms":1700000009000,
				"source":{"db":"d","table":"t","snapshot":"true"}}}`,
			want: CDCEvent{Op: "r", DB: "d", Table: "t", TsMs: 1700000009000, Snapshot: true},
		},
		{
			name: "snapshot=first is a snapshot event",
			raw: `{"payload":{"op":"r","ts_ms":1700000010000,
				"source":{"db":"d","table":"t","snapshot":"first"}}}`,
			want: CDCEvent{Op: "r", DB: "d", Table: "t", TsMs: 1700000010000, Snapshot: true},
		},
		{
			name: "snapshot marker wins even when op is c",
			raw: `{"payload":{"op":"c","ts_ms":1700000011000,
				"source":{"db":"d","table":"t","snapshot":"last"}}}`,
			want: CDCEvent{Op: "c", DB: "d", Table: "t", TsMs: 1700000011000, Snapshot: true},
		},
		{
			name: "incremental snapshot row is a snapshot event",
			raw: `{"payload":{"op":"r","ts_ms":1700000012000,
				"source":{"db":"d","table":"t","snapshot":"incremental"}}}`,
			want: CDCEvent{Op: "r", DB: "d", Table: "t", TsMs: 1700000012000, Snapshot: true},
		},
		{
			name: "snapshot=false is a live event",
			raw: `{"payload":{"op":"c","ts_ms":1700000013000,
				"source":{"db":"d","table":"t","snapshot":"false"}}}`,
			want: CDCEvent{Op: "c", DB: "d", Table: "t", TsMs: 1700000013000},
		},
		{
			name: "un-wrapped envelope snapshot marker",
			raw:  `{"op":"c","ts_ms":1700000014000,"source":{"db":"d","table":"t","snapshot":"true"}}`,
			want: CDCEvent{Op: "c", DB: "d", Table: "t", TsMs: 1700000014000, Snapshot: true},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Parse([]byte(tc.raw))
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestParseHeartbeat covers ADR-018. The first body is the one Debezium Server 3.6.3 sent over
// the HTTP sink in bench/experiments/heartbeat; the rest are records that must NOT be taken for
// a heartbeat, because they would then be dropped from lag, counters and history.
func TestParseHeartbeat(t *testing.T) {
	tests := []struct {
		name          string
		raw           string
		wantHeartbeat bool
		wantOp        string
	}{
		{
			name: "heartbeat with schema envelope (as sent by Debezium Server 3.6.3)",
			raw: `{"schema":{"type":"struct","fields":[{"type":"int64","optional":false,"field":"ts_ms"}],
				"optional":false,"name":"io.debezium.connector.common.Heartbeat","version":1},
				"payload":{"ts_ms":1794027479832}}`,
			wantHeartbeat: true,
		},
		{name: "heartbeat without envelope", raw: `{"ts_ms":1794027479832}`, wantHeartbeat: true},
		{name: "change with op but no source", raw: `{"payload":{"op":"c","ts_ms":1}}`, wantOp: "c"},
		{name: "record with source but no op", raw: `{"payload":{"ts_ms":1,"source":{"db":"d","table":"t"}}}`},
		{name: "record with a row image but no op", raw: `{"payload":{"ts_ms":1,"after":{"id":1}}}`},
		{name: "record with a before image but no op", raw: `{"payload":{"ts_ms":1,"before":{"id":1}}}`},
		{name: "ddl envelope", raw: `{"payload":{"ddl":"ALTER TABLE t ADD COLUMN c int","ts_ms":1}}`},
		{name: "snapshot read", raw: `{"payload":{"op":"r","ts_ms":1,"source":{"db":"d","table":"t","snapshot":"true"}}}`, wantOp: "r"},
		{name: "empty object has no timestamp", raw: `{}`},
		{name: "envelope without a timestamp", raw: `{"payload":{}}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Parse([]byte(tc.raw))
			require.NoError(t, err)
			assert.Equal(t, tc.wantHeartbeat, got.Heartbeat)
			if tc.wantOp != "" {
				assert.Equal(t, tc.wantOp, got.Op)
			}
			if tc.wantHeartbeat {
				assert.Empty(t, got.DB)
				assert.Empty(t, got.Table)
				assert.NotZero(t, got.TsMs)
			}
		})
	}
}

func TestParseMalformedDoesNotPanic(t *testing.T) {
	bad := [][]byte{
		[]byte(`{not json`),
		[]byte(``),
		[]byte(`not even close`),
		[]byte(`{"payload":`),
		nil,
	}
	for _, b := range bad {
		assert.NotPanics(t, func() {
			ev, err := Parse(b)
			require.Error(t, err)
			assert.True(t, errors.Is(err, ErrMalformedEvent), "want ErrMalformedEvent, got %v", err)
			assert.Equal(t, CDCEvent{}, ev)
		})
	}
}
