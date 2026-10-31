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

// Command inspector reads facts about a database with read-only queries and reports them as JSON
// in the container's termination message, which the operator copies into the sensor's status.
//
// It runs in a Job in the sensor's namespace, with the password Secret mounted by Kubernetes, so
// the operator never handles the password and the inspection has the same network path as the
// connector. It needs no Kubernetes API access and writes nothing to the database.
//
// Configuration, all from environment variables:
//
//	INSPECT_ENGINE           postgresql (default); any other value is rejected
//	INSPECT_HOST, INSPECT_PORT, INSPECT_USER, INSPECT_DATABASE
//	INSPECT_SSLMODE          a libpq sslmode; empty means prefer
//	DATABASE_password        the password (a Secret exposed with envFrom prefix DATABASE_)
//	INSPECT_TABLES           comma-separated tables, "schema.table" or a bare name in public
//	INSPECT_SLOT             the connector's replication slot; empty skips the slot facts
//	INSPECT_TIMEOUT_SECONDS  overall deadline (default 25)
//	INSPECT_RESULT_PATH      where to write the result (default /dev/termination-log)
//
// Exit codes: 0 ok, 2 invalid configuration, 3 connection failed, 4 authentication failed,
// 5 inspection failed, 6 result could not be written. A failed inspection still writes a result
// with an error code, so the reason is available even when the exit code is not enough.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/mercadoalex/db-sensor-operator/internal/snapshot"
)

const (
	defaultTimeout    = 25 * time.Second
	defaultResultPath = "/dev/termination-log"
	passwordVar       = "DATABASE_password"
)

type config struct {
	conn       snapshot.ConnectionRef
	tables     []string
	slot       string
	timeout    time.Duration
	resultPath string
}

func loadConfig(getenv func(string) string) (config, error) {
	if engine := getenv("INSPECT_ENGINE"); engine != "" && engine != "postgresql" {
		return config{}, fmt.Errorf("unsupported engine %q: only postgresql is implemented", engine)
	}
	port, err := strconv.Atoi(getenv("INSPECT_PORT"))
	if err != nil || port < 1 || port > 65535 {
		return config{}, errors.New("INSPECT_PORT must be a port number")
	}
	c := config{
		conn: snapshot.ConnectionRef{
			Host: getenv("INSPECT_HOST"), Port: int32(port), User: getenv("INSPECT_USER"),
			Password: getenv(passwordVar), Database: getenv("INSPECT_DATABASE"), SSLMode: getenv("INSPECT_SSLMODE"),
		},
		slot:       getenv("INSPECT_SLOT"),
		timeout:    defaultTimeout,
		resultPath: getenv("INSPECT_RESULT_PATH"),
	}
	if c.conn.Host == "" || c.conn.User == "" || c.conn.Database == "" {
		return config{}, errors.New("INSPECT_HOST, INSPECT_USER and INSPECT_DATABASE are required")
	}
	if c.resultPath == "" {
		c.resultPath = defaultResultPath
	}
	if raw := getenv("INSPECT_TIMEOUT_SECONDS"); raw != "" {
		secs, err := strconv.Atoi(raw)
		if err != nil || secs < 1 {
			return config{}, errors.New("INSPECT_TIMEOUT_SECONDS must be a positive number")
		}
		c.timeout = time.Duration(secs) * time.Second
	}
	for t := range strings.SplitSeq(getenv("INSPECT_TABLES"), ",") {
		if t = strings.TrimSpace(t); t != "" {
			c.tables = append(c.tables, t)
		}
	}
	return c, nil
}

// writeFunc stores the encoded result at a path.
type writeFunc func(path string, data []byte) error

// run performs one inspection and returns the process exit code. The inspector and the writer are
// parameters so that it can be tested without a database or a filesystem.
func run(getenv func(string) string, ins snapshot.Inspector, write writeFunc, logf func(string, ...any)) int {
	password := getenv(passwordVar)

	// output encodes a result and writes it, returning the exit code to use if that fails.
	output := func(path string, r snapshot.Result) int {
		data, err := snapshot.Encode(r)
		if err != nil {
			logf("inspector: %v", err)
			return snapshot.ExitOutput
		}
		if err := write(path, data); err != nil {
			logf("inspector: could not write the result: %v", err)
			return snapshot.ExitOutput
		}
		return snapshot.ExitOK
	}

	cfg, err := loadConfig(getenv)
	if err != nil {
		logf("inspector: invalid configuration: %v", err)
		path := getenv("INSPECT_RESULT_PATH")
		if path == "" {
			path = defaultResultPath
		}
		invalid := snapshot.NewErrorResult(snapshot.ErrCodeConfig, err.Error(), password)
		if code := output(path, invalid); code != snapshot.ExitOK {
			return code
		}
		return snapshot.ExitConfig
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()

	facts, err := ins.Inspect(ctx, cfg.conn, snapshot.Request{Tables: cfg.tables, Slot: cfg.slot})
	if err != nil {
		code, exit := snapshot.Classify(err)
		logf("inspector: failed: %s", code)
		failed := snapshot.NewErrorResult(code, err.Error(), password)
		if out := output(cfg.resultPath, failed); out != snapshot.ExitOK {
			return out
		}
		return exit
	}

	if code := output(cfg.resultPath, snapshot.NewResult(facts)); code != snapshot.ExitOK {
		return code
	}
	logf("inspector: ok, %d tables", len(facts.Tables))
	return snapshot.ExitOK
}

func main() {
	logf := func(format string, args ...any) { fmt.Fprintf(os.Stderr, format+"\n", args...) }
	write := writeFunc(func(path string, data []byte) error { return os.WriteFile(path, data, 0o644) })
	os.Exit(run(os.Getenv, snapshot.PostgresInspector{}, write, logf))
}
