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

package snapshot

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	secretPassword = "s3cr3t/p@ss word"
	sslDisable     = "disable"
)

func TestConnectionRefNeverPrintsPassword(t *testing.T) {
	c := ConnectionRef{Host: "db", Port: 5432, User: "u", Password: secretPassword, Database: "d"}
	for _, s := range []string{fmt.Sprint(c), fmt.Sprintf("%v", c), fmt.Sprintf("%+v", c), fmt.Sprintf("%#v", c), c.String()} {
		assert.NotContains(t, s, secretPassword)
		assert.NotContains(t, s, "s3cr3t")
	}
	assert.Equal(t, "u@db:5432/d", c.String())
}

func TestConnConfig(t *testing.T) {
	cfg, err := PostgresInspector{}.connConfig(ConnectionRef{Host: "db", Port: 5432, User: "u", Password: secretPassword, Database: "d", SSLMode: sslDisable})
	require.NoError(t, err)
	assert.Equal(t, "db", cfg.Host)
	assert.Equal(t, uint16(5432), cfg.Port)
	assert.Equal(t, "d", cfg.Database)
	// The special characters in the password survive URL escaping.
	assert.Equal(t, secretPassword, cfg.Password)
	// The session is read-only and bounded.
	assert.Equal(t, "on", cfg.RuntimeParams["default_transaction_read_only"])
	assert.Equal(t, "5000", cfg.RuntimeParams["statement_timeout"])
	assert.Equal(t, inspectConnectTimeout, cfg.ConnectTimeout)
}

func TestConnConfigRequiresTheEssentials(t *testing.T) {
	for _, c := range []ConnectionRef{{User: "u", Database: "d"}, {Host: "h", Database: "d"}, {Host: "h", User: "u"}} {
		_, err := PostgresInspector{}.connConfig(c)
		assert.Error(t, err)
	}
}

func TestSplitTable(t *testing.T) {
	s, n := splitTable("orders")
	assert.Equal(t, "public", s)
	assert.Equal(t, "orders", n)
	s, n = splitTable("sales.orders")
	assert.Equal(t, "sales", s)
	assert.Equal(t, "orders", n)
}

func TestRedact(t *testing.T) {
	err := redact(errors.New("failed: password "+secretPassword+" rejected"), secretPassword)
	assert.NotContains(t, err.Error(), secretPassword)
	assert.Contains(t, err.Error(), "[redacted]")
	assert.NoError(t, redact(nil, secretPassword))
	orig := errors.New("plain")
	assert.Equal(t, orig, redact(orig, secretPassword))
}

func TestInspectUnreachableDoesNotLeakPassword(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	// Port 1 on localhost refuses connections.
	_, err := PostgresInspector{}.Inspect(ctx, ConnectionRef{Host: "127.0.0.1", Port: 1, User: "u", Password: secretPassword, Database: "d", SSLMode: sslDisable}, Request{Tables: []string{"t"}})
	require.Error(t, err)
	assert.NotContains(t, err.Error(), secretPassword)
}

// TestInspectAgainstPostgres runs against a real PostgreSQL. It is skipped unless
// DBSENSOR_TEST_PG_HOST is set, so the normal test loop needs no database.
//
//	DBSENSOR_TEST_PG_HOST=127.0.0.1 DBSENSOR_TEST_PG_PORT=5432 DBSENSOR_TEST_PG_USER=bench \
//	DBSENSOR_TEST_PG_PASSWORD=... DBSENSOR_TEST_PG_DB=bench go test ./internal/snapshot -run Against -v
//
// It expects the benchmark schema (public.orders, public.users, public.payments).
func TestInspectAgainstPostgres(t *testing.T) {
	host := os.Getenv("DBSENSOR_TEST_PG_HOST")
	if host == "" {
		t.Skip("DBSENSOR_TEST_PG_HOST not set")
	}
	port, _ := strconv.Atoi(os.Getenv("DBSENSOR_TEST_PG_PORT"))
	conn := ConnectionRef{
		Host: host, Port: int32(port), User: os.Getenv("DBSENSOR_TEST_PG_USER"),
		Password: os.Getenv("DBSENSOR_TEST_PG_PASSWORD"), Database: os.Getenv("DBSENSOR_TEST_PG_DB"), SSLMode: sslDisable,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	facts, err := PostgresInspector{}.Inspect(ctx, conn, Request{Tables: []string{tblOrders, "orders", "public.does_not_exist"}})
	require.NoError(t, err)
	got := facts.Tables
	require.Len(t, got, 3)

	assert.True(t, got[0].Found)
	assert.True(t, got[1].Found, "a bare name is looked up in public")
	assert.False(t, got[2].Found)

	assert.Equal(t, walLogical, got[0].WALLevel)
	assert.False(t, got[0].ReadOnlyServer)
	// DBSENSOR_TEST_PG_EXPECT_WRITE=false is for a user without CREATE on the schema.
	assert.Equal(t, os.Getenv("DBSENSOR_TEST_PG_EXPECT_WRITE") != "false", got[0].CanWrite)
	assert.GreaterOrEqual(t, got[0].EstimatedRows, UnknownRows)

	for _, f := range got {
		t.Logf("facts: %+v -> %+v", f, Decide(f, PolicyConfig{}, Capabilities{}))
	}
}

// TestInspectorSessionIsReadOnly proves the session the inspector opens refuses
// writes, using the same config Inspect uses. Skipped without a database; it needs a
// user that could otherwise write (the benchmark user).
func TestInspectorSessionIsReadOnly(t *testing.T) {
	host := os.Getenv("DBSENSOR_TEST_PG_HOST")
	if host == "" || os.Getenv("DBSENSOR_TEST_PG_EXPECT_WRITE") == "false" {
		t.Skip("needs DBSENSOR_TEST_PG_HOST and a user with CREATE")
	}
	port, _ := strconv.Atoi(os.Getenv("DBSENSOR_TEST_PG_PORT"))
	cfg, err := PostgresInspector{}.connConfig(ConnectionRef{
		Host: host, Port: int32(port), User: os.Getenv("DBSENSOR_TEST_PG_USER"),
		Password: os.Getenv("DBSENSOR_TEST_PG_PASSWORD"), Database: os.Getenv("DBSENSOR_TEST_PG_DB"), SSLMode: sslDisable,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	db, err := pgx.ConnectConfig(ctx, cfg)
	require.NoError(t, err)
	defer func() { _ = db.Close(ctx) }()

	_, err = db.Exec(ctx, "create table public.snapshot_inspector_must_not_exist (i int)")
	require.Error(t, err, "a write must be refused")
	assert.Contains(t, err.Error(), "read-only")
}

// TestInspectUnanalyzedTable expects public.fresh_unanalyzed to exist and never
// have been analyzed (reltuples = -1). Set DBSENSOR_TEST_PG_UNANALYZED=1 to run it.
func TestInspectUnanalyzedTable(t *testing.T) {
	host := os.Getenv("DBSENSOR_TEST_PG_HOST")
	if host == "" || os.Getenv("DBSENSOR_TEST_PG_UNANALYZED") == "" {
		t.Skip("needs DBSENSOR_TEST_PG_HOST and DBSENSOR_TEST_PG_UNANALYZED=1")
	}
	port, _ := strconv.Atoi(os.Getenv("DBSENSOR_TEST_PG_PORT"))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	got, err := PostgresInspector{}.Inspect(ctx, ConnectionRef{
		Host: host, Port: int32(port), User: os.Getenv("DBSENSOR_TEST_PG_USER"),
		Password: os.Getenv("DBSENSOR_TEST_PG_PASSWORD"), Database: os.Getenv("DBSENSOR_TEST_PG_DB"), SSLMode: sslDisable,
	}, Request{Tables: []string{"public.fresh_unanalyzed"}})
	require.NoError(t, err)
	require.Len(t, got.Tables, 1)
	tbl := got.Tables[0]
	assert.True(t, tbl.Found)
	assert.Equal(t, UnknownRows, tbl.EstimatedRows)
	assert.Equal(t, StrategySignalTable, Decide(tbl, PolicyConfig{}, Capabilities{}).Kind)
}

func TestEstimate(t *testing.T) {
	tests := []struct {
		name         string
		reltuples    float64
		live, mods   int64
		wantRows     int64
		wantReliable bool
	}{
		{name: "analyzed and untouched", reltuples: 151_000, live: 151_000, mods: 0, wantRows: 151_000, wantReliable: true},
		{name: "never analyzed but loaded", reltuples: -1, live: 150_000, mods: 150_000, wantRows: 150_000, wantReliable: false},
		{name: "bulk load after a small ANALYZE", reltuples: 1_000, live: 501_000, mods: 500_000, wantRows: 501_000, wantReliable: false},
		{name: "within autovacuum tolerance", reltuples: 100_000, live: 100_500, mods: 500, wantRows: 100_500, wantReliable: true},
		{name: "exactly at the tolerance is still reliable", reltuples: 100_000, live: 110_050, mods: 10_050, wantRows: 110_050, wantReliable: true},
		{name: "one change past the tolerance", reltuples: 100_000, live: 110_051, mods: 10_051, wantRows: 110_051, wantReliable: false},
		{name: "modification counter unavailable", reltuples: 5_000, live: 5_000, mods: UnknownRows, wantRows: 5_000, wantReliable: false},
		{name: "nothing known", reltuples: -1, live: 0, mods: UnknownRows, wantRows: UnknownRows, wantReliable: false},
		{name: "small analyzed table", reltuples: 20, live: 20, mods: 3, wantRows: 20, wantReliable: true},
		{name: "live counter reset to zero keeps reltuples", reltuples: 500_000, live: 0, mods: 0, wantRows: 500_000, wantReliable: true},
		{name: "the larger of the two wins", reltuples: 90_000, live: 95_000, mods: 100, wantRows: 95_000, wantReliable: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rows, reliable := estimate(tc.reltuples, tc.live, tc.mods)
			assert.Equal(t, tc.wantRows, rows)
			assert.Equal(t, tc.wantReliable, reliable)
		})
	}
}

// TestInspectStaleStatistics expects public.stale_estimate to exist with a small old
// reltuples and many rows loaded since (autovacuum disabled on it). Set
// DBSENSOR_TEST_PG_STALE=1 to run it.
func TestInspectStaleStatistics(t *testing.T) {
	host := os.Getenv("DBSENSOR_TEST_PG_HOST")
	if host == "" || os.Getenv("DBSENSOR_TEST_PG_STALE") == "" {
		t.Skip("needs DBSENSOR_TEST_PG_HOST and DBSENSOR_TEST_PG_STALE=1")
	}
	port, _ := strconv.Atoi(os.Getenv("DBSENSOR_TEST_PG_PORT"))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	got, err := PostgresInspector{}.Inspect(ctx, ConnectionRef{
		Host: host, Port: int32(port), User: os.Getenv("DBSENSOR_TEST_PG_USER"),
		Password: os.Getenv("DBSENSOR_TEST_PG_PASSWORD"), Database: os.Getenv("DBSENSOR_TEST_PG_DB"), SSLMode: sslDisable,
	}, Request{Tables: []string{"public.stale_estimate", "public.orders"}})
	require.NoError(t, err)
	require.Len(t, got.Tables, 2)

	stale, fresh := got.Tables[0], got.Tables[1]
	t.Logf("stale: %+v", stale)
	t.Logf("fresh: %+v", fresh)
	assert.True(t, stale.Found)
	assert.False(t, stale.EstimateReliable, "reltuples is small but many rows changed since ANALYZE")
	assert.Greater(t, stale.ModsSinceAnalyze, int64(10_000))
	assert.Greater(t, stale.EstimatedRows, DefaultSmallTableRows, "n_live_tup exposes the real size")
	assert.NotEqual(t, StrategyStreamOnly, Decide(stale, PolicyConfig{}, Capabilities{}).Kind)

	assert.True(t, fresh.EstimateReliable, "orders was analyzed and is untouched")
}

// TestInspectSlotAgainstPostgres inspects the replication slot named by DBSENSOR_TEST_PG_SLOT
// and logs the facts and the diagnosis. Skipped without a database and a slot name.
func TestInspectSlotAgainstPostgres(t *testing.T) {
	host, slotName := os.Getenv("DBSENSOR_TEST_PG_HOST"), os.Getenv("DBSENSOR_TEST_PG_SLOT")
	if host == "" || slotName == "" {
		t.Skip("needs DBSENSOR_TEST_PG_HOST and DBSENSOR_TEST_PG_SLOT")
	}
	port, _ := strconv.Atoi(os.Getenv("DBSENSOR_TEST_PG_PORT"))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	got, err := PostgresInspector{}.Inspect(ctx, ConnectionRef{
		Host: host, Port: int32(port), User: os.Getenv("DBSENSOR_TEST_PG_USER"),
		Password: os.Getenv("DBSENSOR_TEST_PG_PASSWORD"), Database: os.Getenv("DBSENSOR_TEST_PG_DB"), SSLMode: sslDisable,
	}, Request{Slot: slotName})
	require.NoError(t, err)
	require.NotNil(t, got.Slot)
	risk := DiagnoseSlot(got.Server, got.Slot)
	t.Logf("server: %+v", got.Server)
	t.Logf("slot:   %+v", *got.Slot)
	t.Logf("risk:   %s | %s", risk.Level, risk.Reason)
	assert.True(t, got.Slot.Found)
}

func TestEstimatePartitioned(t *testing.T) {
	tests := []struct {
		name         string
		reltuples    float64
		leafLive     int64
		wantRows     int64
		wantReliable bool
	}{
		{name: "parent and leaves agree", reltuples: 300_000, leafLive: 300_000, wantRows: 300_000, wantReliable: true},
		{name: "within tolerance", reltuples: 100_000, leafLive: 105_000, wantRows: 105_000, wantReliable: true},
		{name: "a small parent that was loaded afterwards", reltuples: 1_000, leafLive: 600_000, wantRows: 600_000, wantReliable: false},
		{name: "the parent was never analyzed", reltuples: -1, leafLive: 300_000, wantRows: 300_000, wantReliable: false},
		{name: "leaf counters were reset", reltuples: 500_000, leafLive: 0, wantRows: 500_000, wantReliable: false},
		{name: "the leaf statistics are unavailable", reltuples: 5_000, leafLive: -1, wantRows: 5_000, wantReliable: false},
		{name: "rows were deleted since the parent was analyzed", reltuples: 500_000, leafLive: 10_000, wantRows: 500_000, wantReliable: false},
		{name: "an empty partitioned table", reltuples: 0, leafLive: 0, wantRows: 0, wantReliable: true},
		{name: "nothing known", reltuples: -1, leafLive: 0, wantRows: UnknownRows, wantReliable: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rows, reliable := estimatePartitioned(tc.reltuples, tc.leafLive)
			assert.Equal(t, tc.wantRows, rows)
			assert.Equal(t, tc.wantReliable, reliable)
		})
	}
}

// TestInspectPartitionedAgainstPostgres expects public.part_small to be a partitioned table
// analyzed at about 1,000 rows and loaded to far more since. Set DBSENSOR_TEST_PG_PARTITIONED=1.
func TestInspectPartitionedAgainstPostgres(t *testing.T) {
	host := os.Getenv("DBSENSOR_TEST_PG_HOST")
	if host == "" || os.Getenv("DBSENSOR_TEST_PG_PARTITIONED") == "" {
		t.Skip("needs DBSENSOR_TEST_PG_HOST and DBSENSOR_TEST_PG_PARTITIONED=1")
	}
	port, _ := strconv.Atoi(os.Getenv("DBSENSOR_TEST_PG_PORT"))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	got, err := PostgresInspector{}.Inspect(ctx, ConnectionRef{
		Host: host, Port: int32(port), User: os.Getenv("DBSENSOR_TEST_PG_USER"),
		Password: os.Getenv("DBSENSOR_TEST_PG_PASSWORD"), Database: os.Getenv("DBSENSOR_TEST_PG_DB"), SSLMode: sslDisable,
	}, Request{Tables: []string{"public.part_small"}})
	require.NoError(t, err)
	require.Len(t, got.Tables, 1)
	f := got.Tables[0]
	t.Logf("part_small: %+v", f)
	assert.Greater(t, f.EstimatedRows, DefaultSmallTableRows, "the leaves expose the real size")
	assert.False(t, f.EstimateReliable)
	assert.NotEqual(t, StrategyStreamOnly, Decide(f, PolicyConfig{}, Capabilities{}).Kind)
}
