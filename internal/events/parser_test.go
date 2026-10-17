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
			want: CDCEvent{Op: "c", DB: "orders_db", Table: "orders", TsMs: 1700000000000},
		},
		{
			name: "update",
			raw: `{"payload":{"op":"u","ts_ms":1700000001000,
				"source":{"db":"orders_db","table":"customers"}}}`,
			want: CDCEvent{Op: "u", DB: "orders_db", Table: "customers", TsMs: 1700000001000},
		},
		{
			name: "delete",
			raw: `{"payload":{"op":"d","ts_ms":1700000002000,
				"source":{"db":"orders_db","table":"orders"}}}`,
			want: CDCEvent{Op: "d", DB: "orders_db", Table: "orders", TsMs: 1700000002000},
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
			want: CDCEvent{Op: "ddl", DB: "orders_db", Table: "orders", TsMs: 1700000004000},
		},
		{
			name: "ddl via schema-history historyRecord marker",
			raw: `{"payload":{"historyRecord":"{}","ts_ms":1700000005000,
				"source":{"db":"orders_db","table":"orders"}}}`,
			want: CDCEvent{Op: "ddl", DB: "orders_db", Table: "orders", TsMs: 1700000005000},
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Parse([]byte(tc.raw))
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
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
		b := b
		assert.NotPanics(t, func() {
			ev, err := Parse(b)
			require.Error(t, err)
			assert.True(t, errors.Is(err, ErrMalformedEvent), "want ErrMalformedEvent, got %v", err)
			assert.Equal(t, CDCEvent{}, ev)
		})
	}
}
