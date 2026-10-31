# CDC Observability Benchmark harness

A reproducible, local-only harness for comparing four ways to get data-activity
(mutation) observability from PostgreSQL: CDC-derived (Lactiflluus over
Debezium), polling (`pg_stat_user_tables`), triggers, and `pg_stat_statements`.

The benchmark's design is not versioned in this repository. The report will
live in `docs/benchmark/`. This directory holds the harness only.

Like `demo/`, it needs no cloud account or paid service. You need Docker, kind
and kubectl:

```bash
brew install kind kubectl
```

## Status

Done: cluster setup, the workload generator, and the CDC collector
(`collectors/cdc/`). The polling, triggers and statement-stats collectors, the
measurement harness and the report come in later tasks. `make collect` and
`make report` are placeholders until then.

## Quick start

```bash
cd bench
make setup      # kind cluster + PostgreSQL (wal_level=logical) + schema
make run        # deterministic workload, default 50 ops/s for 60s
make cdc-up     # CDC Approach: Debezium Operator + Lactiflluus (see collectors/cdc/)
make cdc-down   # remove it, including the replication slot
make teardown   # delete the kind cluster
```

## Workload generator

`workload/workload.sql` defines `bench.run_workload(...)`, a PL/pgSQL procedure.
It calls `setseed()` first, so the sequence of tables, operations and row
targets is a function of the parameters and the starting data. Every Approach
sees the same mutation sequence.

| Variable     | Default | Meaning                              |
|--------------|---------|--------------------------------------|
| `SEED`       | 42      | Fixed seed (documented, not random)  |
| `INSERT_PCT` | 50      | Insert share of the mix              |
| `UPDATE_PCT` | 40      | Update share of the mix              |
| `DELETE_PCT` | 10      | Delete share of the mix              |
| `RATE`       | 50      | Operations per second                |
| `DURATION`   | 60      | Seconds                              |

The mix must sum to 100. Example:

```bash
make run RATE=200 DURATION=120 INSERT_PCT=70 UPDATE_PCT=20 DELETE_PCT=10
```

Each operation commits on its own. If the database cannot keep up, operations
run late instead of being dropped, and the procedure prints the achieved rate
so it can be disclosed in the report.

Determinism holds for a given starting data state. Run `make teardown` and
`make setup` between runs if you need identical starting rows.

## Layout

```
bench/
├── Makefile      setup, run, collect, report, teardown
├── cluster/      kind config, PostgreSQL manifest, schema
├── workload/     deterministic i/u/d generator
└── results/      run artifacts (raw output is git-ignored)
```

## Fairness notes

- `wal_level=logical` is set for every run, including the no-collector baseline,
  so all Approaches pay the same WAL cost.
- `pg_stat_statements` is not preloaded. Its tracking is always on once enabled
  and cannot be isolated per run, so only the statement-stats collector enables it.
- Everything runs on one kind cluster on one host. Results are comparative under
  identical conditions, not a claim about any production system.
