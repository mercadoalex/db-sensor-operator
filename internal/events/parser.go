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

// Package events is the transport-agnostic CDC event pipeline for a
// DatabaseSensor: it parses Debezium change events into a flat CDCEvent
// (parser.go), consumes them over HTTP (default) or Kafka (consumer.go), and
// fans each one out to the metrics Exporter and the health LagTracker
// (pipeline.go).
//
// The parser is deliberately tolerant: Debezium emits slightly different
// envelope shapes per connector (relational vs MongoDB) and per delivery mode
// (with/without a wrapping "payload" object when the JSON converter's schema is
// enabled). It reads fields by precedence rather than assuming one shape, and it
// never panics on malformed input — bad JSON yields a wrapped error so a single
// poison message cannot take the consumer down (NFR-1 / NFR-6).
package events

import (
	"errors"
	"fmt"

	"github.com/tidwall/gjson"
)

// CDCEvent is the flat, connector-independent view of a single Debezium change
// event that the rest of the pipeline (Exporter, LagTracker) consumes.
//
//   - Op is the change kind: "c" (insert/create), "u" (update), "d" (delete),
//     "t" (truncate), or "ddl" for a schema-change / DDL envelope.
//   - DB is the logical database host or server name (metric label "db").
//   - Table is the changed table or MongoDB collection (metric label "table").
//   - TsMs is the source event time in Unix milliseconds, used to derive lag.
type CDCEvent struct {
	Op    string
	DB    string
	Table string
	TsMs  int64
}

// ErrMalformedEvent is returned by Parse when the input is not valid JSON. It is
// wrapped (via %w) with context so callers can log the cause without the raw
// payload leaking structure, and can errors.Is-match the sentinel.
var ErrMalformedEvent = errors.New("malformed CDC event")

// Parse turns a raw Debezium change-event payload into a CDCEvent. It validates
// the JSON first (gjson does not validate during Get) and returns a wrapped
// ErrMalformedEvent on bad input rather than panicking, so a poison message
// cannot crash the consumer loop.
//
// Field precedence (first present wins), chosen to tolerate the common Debezium
// envelope variants:
//
//	Op:    payload.op            -> op                    (c|u|d|t)
//	       mapped to "ddl" when a DDL/schema-change envelope is detected:
//	       payload.ddl / ddl present, or historyRecord / schemaChange markers,
//	       or op is absent while a DDL field exists.
//	TsMs:  payload.ts_ms         -> ts_ms -> payload.source.ts_ms -> source.ts_ms
//	DB:    payload.source.db     -> source.db -> payload.source.name -> source.name
//	Table: payload.source.table  -> source.table -> payload.source.collection
//	       -> source.collection   (MongoDB reports a collection, not a table)
func Parse(raw []byte) (CDCEvent, error) {
	if !gjson.ValidBytes(raw) {
		return CDCEvent{}, fmt.Errorf("%w: invalid JSON", ErrMalformedEvent)
	}

	root := gjson.ParseBytes(raw)

	ev := CDCEvent{
		Op:    deriveOp(root),
		DB:    firstString(root, "payload.source.db", "source.db", "payload.source.name", "source.name"),
		Table: firstString(root, "payload.source.table", "source.table", "payload.source.collection", "source.collection"),
		TsMs:  firstInt(root, "payload.ts_ms", "ts_ms", "payload.source.ts_ms", "source.ts_ms"),
	}
	return ev, nil
}

// deriveOp resolves the change kind, mapping any schema-change / DDL envelope to
// "ddl" (US-2.4). A DDL envelope is recognised by a non-empty ddl field or by
// Debezium schema-history markers; otherwise the connector op (c|u|d|t) is used.
func deriveOp(root gjson.Result) string {
	if root.Get("payload.ddl").Exists() || root.Get("ddl").Exists() ||
		root.Get("payload.historyRecord").Exists() || root.Get("historyRecord").Exists() ||
		root.Get("payload.schemaChange").Exists() || root.Get("schemaChange").Exists() {
		return "ddl"
	}
	return firstString(root, "payload.op", "op")
}

// firstString returns the first non-empty string value among the given gjson
// paths, or "" if none are present.
func firstString(root gjson.Result, paths ...string) string {
	for _, p := range paths {
		if r := root.Get(p); r.Exists() {
			if s := r.String(); s != "" {
				return s
			}
		}
	}
	return ""
}

// firstInt returns the first present integer value among the given gjson paths,
// or 0 if none are present.
func firstInt(root gjson.Result, paths ...string) int64 {
	for _, p := range paths {
		if r := root.Get(p); r.Exists() {
			return r.Int()
		}
	}
	return 0
}
