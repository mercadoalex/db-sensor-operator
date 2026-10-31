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

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mercadoalex/db-sensor-operator/internal/snapshot"
)

const (
	testPassword = "s3cr3t/p@ss word"
	portVar      = "INSPECT_PORT"
)

type fakeInspector struct {
	facts snapshot.Facts
	err   error
	got   snapshot.Request
	conn  snapshot.ConnectionRef
}

func (f *fakeInspector) Inspect(
	_ context.Context, conn snapshot.ConnectionRef, req snapshot.Request,
) (snapshot.Facts, error) {
	f.conn, f.got = conn, req
	return f.facts, f.err
}

type capture struct {
	path string
	data []byte
	err  error
	logs []string
}

func (c *capture) write(path string, data []byte) error {
	if c.err != nil {
		return c.err
	}
	c.path, c.data = path, data
	return nil
}
func (c *capture) logf(format string, args ...any) {
	c.logs = append(c.logs, fmt.Sprintf(format, args...))
}

func env(overrides map[string]string) func(string) string {
	m := map[string]string{
		"INSPECT_HOST": "db", portVar: "5432", "INSPECT_USER": "u", "INSPECT_DATABASE": "d",
		passwordVar: testPassword, "INSPECT_TABLES": "public.orders, users ,", "INSPECT_SLOT": "s1",
	}
	maps.Copy(m, overrides)
	return func(k string) string { return m[k] }
}

func decode(t *testing.T, c *capture) snapshot.Result {
	t.Helper()
	var r snapshot.Result
	require.NoError(t, json.Unmarshal(c.data, &r))
	return r
}

func TestRunSuccess(t *testing.T) {
	ins := &fakeInspector{facts: snapshot.Facts{
		Server: snapshot.ServerFacts{Version: 160015, WALLevel: "logical"},
		Tables: []snapshot.TableFacts{{Table: "public.orders", Found: true, EstimatedRows: 10, EstimateReliable: true}},
	}}
	c := &capture{}
	code := run(env(nil), ins, c.write, c.logf)

	assert.Equal(t, snapshot.ExitOK, code)
	assert.Equal(t, defaultResultPath, c.path, "the result goes to the termination log by default")
	r := decode(t, c)
	assert.Equal(t, snapshot.FactsVersion, r.FactsVersion)
	assert.Nil(t, r.Error)
	assert.Len(t, r.Tables, 1)

	assert.Equal(t, []string{"public.orders", "users"}, ins.got.Tables, "tables are split and trimmed, empties dropped")
	assert.Equal(t, "s1", ins.got.Slot)
	assert.Equal(t, int32(5432), ins.conn.Port)
	assert.Equal(t, testPassword, ins.conn.Password, "the password reaches the inspector")
}

func TestRunNeverLogsOrWritesThePassword(t *testing.T) {
	cause := fmt.Errorf("%w: connecting: dial failed for password %s", snapshot.ErrConnect, testPassword)
	ins := &fakeInspector{err: cause}
	c := &capture{}
	code := run(env(nil), ins, c.write, c.logf)

	assert.Equal(t, snapshot.ExitConnect, code)
	assert.NotContains(t, string(c.data), testPassword)
	assert.NotContains(t, fmt.Sprint(c.logs), testPassword)
	r := decode(t, c)
	require.NotNil(t, r.Error)
	assert.Equal(t, snapshot.ErrCodeConnect, r.Error.Code)
}

func TestRunMapsFailuresToExitCodes(t *testing.T) {
	c := &capture{}
	ins := &fakeInspector{err: errors.New("inspecting table: boom")}
	assert.Equal(t, snapshot.ExitInspection, run(env(nil), ins, c.write, c.logf))
	assert.Equal(t, snapshot.ErrCodeInspection, decode(t, c).Error.Code)
}

func TestRunRejectsBadConfigurationAndStillReportsIt(t *testing.T) {
	for name, overrides := range map[string]map[string]string{
		"bad port":           {portVar: "abc"},
		"port out of range":  {portVar: "70000"},
		"missing host":       {"INSPECT_HOST": ""},
		"unsupported engine": {"INSPECT_ENGINE": "mysql"},
		"bad timeout":        {"INSPECT_TIMEOUT_SECONDS": "0"},
	} {
		t.Run(name, func(t *testing.T) {
			ins := &fakeInspector{}
			c := &capture{}
			code := run(env(overrides), ins, c.write, c.logf)
			assert.Equal(t, snapshot.ExitConfig, code)
			assert.Equal(t, snapshot.ErrCodeConfig, decode(t, c).Error.Code, "the reason is still written")
			assert.Empty(t, ins.got.Tables, "no inspection is attempted")
		})
	}
}

func TestRunReportsAnUnwritableResult(t *testing.T) {
	c := &capture{err: errors.New("read-only file system")}
	assert.Equal(t, snapshot.ExitOutput, run(env(nil), &fakeInspector{}, c.write, c.logf))
}

func TestRunHonorsAResultPathOverride(t *testing.T) {
	c := &capture{}
	run(env(map[string]string{"INSPECT_RESULT_PATH": "/tmp/out.json"}), &fakeInspector{}, c.write, c.logf)
	assert.Equal(t, "/tmp/out.json", c.path)
}
