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
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// ConnectionRef is what an Inspector needs to reach the database. Password is a
// secret: the String and GoString methods omit it so that logging a ConnectionRef
// with %v, %+v or %#v never leaks it.
type ConnectionRef struct {
	Host     string
	Port     int32
	User     string
	Password string
	Database string
	// SSLMode is a libpq sslmode (disable, require, verify-full, ...). Empty means
	// "prefer", libpq's own default.
	SSLMode string
}

// String implements fmt.Stringer without the password.
func (c ConnectionRef) String() string {
	return fmt.Sprintf("%s@%s:%d/%s", c.User, c.Host, c.Port, c.Database)
}

// GoString implements fmt.GoStringer without the password.
func (c ConnectionRef) GoString() string { return "snapshot.ConnectionRef{" + c.String() + "}" }

// Request says what to inspect.
type Request struct {
	// Tables are the monitored tables ("schema.table" or a bare name in public).
	Tables []string
	// Slot is the replication slot the connector uses. Empty skips the slot facts.
	Slot string
}

// Facts is everything one inspection learned.
type Facts struct {
	Server ServerFacts
	// Slot is nil when Request.Slot was empty or the server cannot be asked
	// (a standby has no slots of its own to inspect).
	Slot   *SlotFacts
	Tables []TableFacts
}

// Inspector reads facts about the server, the replication slot and the monitored
// tables. Implementations must use only read-only queries.
type Inspector interface {
	Inspect(ctx context.Context, conn ConnectionRef, req Request) (Facts, error)
}

const (
	inspectConnectTimeout   = 5 * time.Second
	inspectStatementTimeout = "5000" // milliseconds, applied to every statement
)

// PostgresInspector is the PostgreSQL Inspector.
type PostgresInspector struct{}

// connConfig builds the pgx config. The password is placed in the URL's userinfo,
// which url.UserPassword escapes, and never formatted into an error message.
func (PostgresInspector) connConfig(c ConnectionRef) (*pgx.ConnConfig, error) {
	if c.Host == "" || c.User == "" || c.Database == "" {
		return nil, errors.New("connection needs host, user and database")
	}
	u := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(c.User, c.Password),
		Host:   c.Host + ":" + strconv.Itoa(int(c.Port)),
		Path:   "/" + c.Database,
	}
	if c.SSLMode != "" {
		q := url.Values{}
		q.Set("sslmode", c.SSLMode)
		u.RawQuery = q.Encode()
	}
	cfg, err := pgx.ParseConfig(u.String())
	if err != nil {
		// pgx errors can echo the connection string, which carries the password.
		return nil, errors.New("invalid connection settings")
	}
	cfg.ConnectTimeout = inspectConnectTimeout
	// Defense in depth: the session is read-only, so even a bug here cannot write.
	cfg.RuntimeParams["default_transaction_read_only"] = "on"
	cfg.RuntimeParams["statement_timeout"] = inspectStatementTimeout
	return cfg, nil
}

// Inspect reads the server state once, then the replication slot (if requested) and
// the row estimate and schema privilege of each table. It returns one TableFacts per
// requested table, in order. A table that does not exist yields Found=false and no
// error, and so does a missing slot.
func (p PostgresInspector) Inspect(ctx context.Context, conn ConnectionRef, req Request) (Facts, error) {
	cfg, err := p.connConfig(conn)
	if err != nil {
		return Facts{}, err
	}
	db, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return Facts{}, fmt.Errorf("%w: connecting to %s: %w", ErrConnect, conn, redact(err, conn.Password))
	}
	defer func() { _ = db.Close(ctx) }()

	server, err := inspectServer(ctx, db)
	if err != nil {
		return Facts{}, fmt.Errorf("reading server state: %w", redact(err, conn.Password))
	}
	facts := Facts{Server: server}

	if req.Slot != "" && !server.InRecovery {
		slot, err := inspectSlot(ctx, db, server, req.Slot)
		if err != nil {
			return Facts{}, fmt.Errorf("inspecting replication slot: %w", redact(err, conn.Password))
		}
		facts.Slot = &slot
	}

	out := make([]TableFacts, 0, len(req.Tables))
	for _, t := range req.Tables {
		schema, name := splitTable(t)
		f := TableFacts{Table: t, ReadOnlyServer: server.InRecovery, WALLevel: server.WALLevel}

		// relkind r = table, p = partitioned table. reltuples is -1 until the table
		// has been analyzed (PostgreSQL 14+) and 0 on older servers for the same case.
		// n_live_tup and n_mod_since_analyze come from the statistics system; a
		// partitioned parent has no row there, so they are -1 (left join).
		var tuples float64
		var canCreate bool
		var liveTuples, mods int64
		err := db.QueryRow(ctx, `
			select c.reltuples, has_schema_privilege(current_user, n.oid, 'CREATE'),
			       coalesce(s.n_live_tup, -1), coalesce(s.n_mod_since_analyze, -1)
			from pg_class c
			join pg_namespace n on n.oid = c.relnamespace
			left join pg_stat_user_tables s on s.relid = c.oid
			where n.nspname = $1 and c.relname = $2 and c.relkind in ('r', 'p')`,
			schema, name).Scan(&tuples, &canCreate, &liveTuples, &mods)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			// Found stays false.
		case err != nil:
			return Facts{}, fmt.Errorf("inspecting table %q: %w", t, redact(err, conn.Password))
		default:
			f.Found = true
			f.CanWrite = canCreate
			f.EstimatedRows, f.EstimateReliable = estimate(tuples, liveTuples, mods)
			f.ModsSinceAnalyze = mods
		}
		out = append(out, f)
	}
	facts.Tables = out
	return facts, nil
}

// inspectServer reads the version, recovery state, wal_level and the slot retention cap.
func inspectServer(ctx context.Context, db *pgx.Conn) (ServerFacts, error) {
	var s ServerFacts
	if err := db.QueryRow(ctx,
		`select current_setting('server_version_num')::int, pg_is_in_recovery(), current_setting('wal_level')`).
		Scan(&s.Version, &s.InRecovery, &s.WALLevel); err != nil {
		return ServerFacts{}, err
	}

	// max_slot_wal_keep_size exists from PostgreSQL 13. Read it from pg_settings, where the
	// value is a number in `unit` (-1 means unlimited); current_setting() would return text
	// such as "5GB" that cannot be cast to a number.
	if s.Version >= 130000 {
		var value int64
		var unit *string
		err := db.QueryRow(ctx, `select setting::bigint, unit from pg_settings where name = 'max_slot_wal_keep_size'`).Scan(&value, &unit)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
		case err != nil:
			return ServerFacts{}, err
		case value < 0:
			s.MaxSlotWALKeepSizeKnown, s.MaxSlotWALKeepSizeBytes = true, UnlimitedBytes
		case unit != nil && *unit == "MB":
			s.MaxSlotWALKeepSizeKnown, s.MaxSlotWALKeepSizeBytes = true, value*1024*1024
		}
	}
	return s, nil
}

// inspectSlot reads the state of one replication slot. Retained WAL is measured from the
// slot's restart_lsn to the current WAL position. wal_status and safe_wal_size exist from
// PostgreSQL 13.
func inspectSlot(ctx context.Context, db *pgx.Conn, server ServerFacts, name string) (SlotFacts, error) {
	slot := SlotFacts{Name: name, RetainedBytes: UnknownBytes, ConfirmedLagBytes: UnknownBytes, SafeWALBytes: UnknownBytes}

	var active bool
	var retained, confirmedLag *int64
	var walStatus *string
	var safe *int64
	var err error
	if server.Version >= 130000 {
		err = db.QueryRow(ctx, `
			select active, wal_status, safe_wal_size,
			       case when restart_lsn is null then null
			            else pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint end,
			       case when confirmed_flush_lsn is null then null
			            else pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint end
			from pg_replication_slots where slot_name = $1`, name).Scan(&active, &walStatus, &safe, &retained, &confirmedLag)
	} else {
		err = db.QueryRow(ctx, `
			select active,
			       case when restart_lsn is null then null
			            else pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint end,
			       case when confirmed_flush_lsn is null then null
			            else pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint end
			from pg_replication_slots where slot_name = $1`, name).Scan(&active, &retained, &confirmedLag)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return slot, nil // Found stays false
	}
	if err != nil {
		return SlotFacts{}, err
	}
	slot.Found, slot.Active = true, active
	if walStatus != nil {
		slot.WALStatus = *walStatus
	}
	if retained != nil {
		slot.RetainedBytes = *retained
	}
	if confirmedLag != nil {
		slot.ConfirmedLagBytes = *confirmedLag
	}
	if safe != nil {
		slot.SafeWALBytes = *safe
	}
	return slot, nil
}

// Autovacuum re-analyzes a table once more than
// autovacuumAnalyzeThreshold + autovacuumAnalyzeScaleFactor*reltuples rows changed
// since the last ANALYZE. These are PostgreSQL's defaults for
// autovacuum_analyze_threshold and autovacuum_analyze_scale_factor, and the same test
// decides here whether the planner's estimate is stale.
//
// Known limitations (documented in ADR-014, not bugs):
//   - The server's own settings and per-table storage parameters are not read. A server
//     with looser settings makes this test flag staleness earlier than autovacuum would
//     (conservative); one with stricter settings makes it more lenient than autovacuum,
//     by at most about 10% of reltuples, which the max with n_live_tup narrows.
//   - n_mod_since_analyze and n_live_tup restart from zero after crash recovery or
//     pg_stat_reset(), so a stale reltuples can look reliable until the next ANALYZE.
const (
	autovacuumAnalyzeThreshold   = 50
	autovacuumAnalyzeScaleFactor = 0.1
)

// estimate turns the raw statistics into a row estimate and a reliability flag.
//
// The estimate is the larger of reltuples and n_live_tup (overestimating is the safe
// direction). It is reliable only when the table has been analyzed (reltuples >= 0),
// the modification counter is available, and few enough rows changed since the last
// ANALYZE that autovacuum would not yet have re-analyzed it. A bulk load without
// ANALYZE fails the last test even though reltuples then looks like a valid number.
func estimate(reltuples float64, liveTuples, modsSinceAnalyze int64) (rows int64, reliable bool) {
	rows = UnknownRows
	if reltuples >= 0 {
		rows = int64(reltuples)
	}
	if liveTuples > 0 && liveTuples > rows {
		rows = liveTuples
	}
	if reltuples < 0 || modsSinceAnalyze < 0 {
		return rows, false
	}
	staleAfter := float64(autovacuumAnalyzeThreshold) + autovacuumAnalyzeScaleFactor*reltuples
	return rows, float64(modsSinceAnalyze) <= staleAfter
}

// splitTable splits "schema.table" into its parts; a bare name is in "public".
func splitTable(t string) (schema, name string) {
	if schema, name, ok := strings.Cut(t, "."); ok {
		return schema, name
	}
	return "public", t
}

// redact removes the password from an error message, in case a driver error
// echoes it.
func redact(err error, password string) error {
	if err == nil || password == "" {
		return err
	}
	msg := err.Error()
	if !strings.Contains(msg, password) {
		return err
	}
	return errors.New(strings.ReplaceAll(msg, password, "[redacted]"))
}
