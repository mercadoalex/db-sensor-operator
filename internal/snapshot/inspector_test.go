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
	_, err := PostgresInspector{}.Inspect(ctx, ConnectionRef{Host: "127.0.0.1", Port: 1, User: "u", Password: secretPassword, Database: "d", SSLMode: sslDisable}, []string{"t"})
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
	got, err := PostgresInspector{}.Inspect(ctx, conn, []string{"public.orders", "orders", "public.does_not_exist"})
	require.NoError(t, err)
	require.Len(t, got, 3)

	assert.True(t, got[0].Found)
	assert.True(t, got[1].Found, "a bare name is looked up in public")
	assert.False(t, got[2].Found)

	assert.Equal(t, "logical", got[0].WALLevel)
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
	}, []string{"public.fresh_unanalyzed"})
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.True(t, got[0].Found)
	assert.Equal(t, UnknownRows, got[0].EstimatedRows)
	assert.Equal(t, StrategySignalTable, Decide(got[0], PolicyConfig{}, Capabilities{}).Kind)
}
