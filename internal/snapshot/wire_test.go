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
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sampleFacts() Facts {
	return Facts{
		Server: ServerFacts{Version: 160015, WALLevel: walLogical, MaxSlotWALKeepSizeKnown: true, MaxSlotWALKeepSizeBytes: UnlimitedBytes},
		Slot:   &SlotFacts{Name: "s1", Found: true, Active: true, WALStatus: "reserved", RetainedBytes: 0, ConfirmedLagBytes: UnknownBytes, SafeWALBytes: UnknownBytes},
		Tables: []TableFacts{
			{Table: tblOrders, Found: true, EstimatedRows: 151000, EstimateReliable: true, CanWrite: true},
			{Table: "public.unanalyzed", Found: true, EstimatedRows: UnknownRows},
			{Table: "public.missing"},
		},
	}
}

func TestNewResultKeepsAZeroAndDropsAnUnknown(t *testing.T) {
	r := NewResult(sampleFacts())
	assert.Equal(t, FactsVersion, r.FactsVersion)
	require.Len(t, r.Tables, 3)

	require.NotNil(t, r.Tables[0].EstimatedRows)
	assert.Equal(t, int64(151000), *r.Tables[0].EstimatedRows)
	assert.Nil(t, r.Tables[1].EstimatedRows, "an unknown estimate is absent")
	assert.Nil(t, r.Tables[2].EstimatedRows, "a missing table has no estimate")
	assert.False(t, r.Tables[2].Found)

	slot := r.Postgres.ReplicationSlot
	require.NotNil(t, slot)
	require.NotNil(t, slot.RetainedBytes, "a retained 0 is kept, not dropped")
	assert.Equal(t, int64(0), *slot.RetainedBytes)
	assert.Nil(t, slot.ConfirmedLagBytes, "an unread value is absent")
	require.NotNil(t, r.Postgres.MaxSlotWALKeepSize)
	assert.Equal(t, int64(-1), *r.Postgres.MaxSlotWALKeepSize, "-1 means unlimited")
}

func TestNewResultOmitsAMissingSlotAndAnUnknownCap(t *testing.T) {
	f := sampleFacts()
	f.Slot = &SlotFacts{Name: "s1"} // not found
	f.Server.MaxSlotWALKeepSizeKnown = false
	r := NewResult(f)
	assert.Nil(t, r.Postgres.ReplicationSlot, "a slot that does not exist is absent, not a zeroed object")
	assert.Nil(t, r.Postgres.MaxSlotWALKeepSize, "a cap that could not be read is absent")

	f.Slot = nil
	assert.Nil(t, NewResult(f).Postgres.ReplicationSlot)
}

func TestEncodeRoundTrips(t *testing.T) {
	b, err := Encode(NewResult(sampleFacts()))
	require.NoError(t, err)
	var back Result
	require.NoError(t, json.Unmarshal(b, &back))
	assert.Equal(t, FactsVersion, back.FactsVersion)
	assert.Len(t, back.Tables, 3)
	assert.False(t, back.Truncated)
}

func manyTables(n int) Facts {
	f := sampleFacts()
	f.Tables = nil
	for i := range n {
		f.Tables = append(f.Tables, TableFacts{Table: fmt.Sprintf("public.table_number_%04d", i), Found: true, EstimatedRows: int64(1000 + i), EstimateReliable: true, CanWrite: true})
	}
	return f
}

func TestEncodeNeverExceedsTheTerminationMessageLimit(t *testing.T) {
	for _, n := range []int{1, 10, 30, 60, 500, 5000} {
		b, err := Encode(NewResult(manyTables(n)))
		require.NoError(t, err)
		assert.LessOrEqual(t, len(b), MaxResultBytes, "%d tables", n)
		assert.Less(t, MaxResultBytes, 4096, "must stay under what the kubelet keeps")
	}
}

func TestEncodeTruncatesFromTheEndAndSaysSo(t *testing.T) {
	b, err := Encode(NewResult(manyTables(5000)))
	require.NoError(t, err)
	var back Result
	require.NoError(t, json.Unmarshal(b, &back))
	assert.True(t, back.Truncated)
	require.NotEmpty(t, back.Tables)
	assert.Less(t, len(back.Tables), 5000)
	assert.Equal(t, "public.table_number_0000", back.Tables[0].Name, "the first tables are the ones kept")
	assert.NotNil(t, back.Postgres, "server and slot facts survive truncation")

	small, err := Encode(NewResult(manyTables(3)))
	require.NoError(t, err)
	assert.NotContains(t, string(small), "truncated")
}

func TestNewErrorResultScrubsSecretsAndShortensTheMessage(t *testing.T) {
	r := NewErrorResult(ErrCodeConnect, "failed: password "+secretPassword+" rejected", secretPassword)
	assert.NotContains(t, r.Error.Message, secretPassword)
	assert.Contains(t, r.Error.Message, "[redacted]")
	assert.Equal(t, ErrCodeConnect, r.Error.Code)

	long := NewErrorResult(ErrCodeInspection, strings.Repeat("é", 2000))
	assert.LessOrEqual(t, len([]rune(long.Error.Message)), maxErrorMessage+3)

	b, err := Encode(long)
	require.NoError(t, err)
	assert.LessOrEqual(t, len(b), MaxResultBytes)
}

func TestClassify(t *testing.T) {
	code, exit := Classify(fmt.Errorf("%w: connecting: %w", ErrConnect, &pgconn.PgError{Code: "28P01", Message: "password authentication failed"}))
	assert.Equal(t, ErrCodeAuth, code)
	assert.Equal(t, ExitAuth, exit)

	code, exit = Classify(fmt.Errorf("%w: connecting: %w", ErrConnect, errors.New("connection refused")))
	assert.Equal(t, ErrCodeConnect, code)
	assert.Equal(t, ExitConnect, exit)

	code, exit = Classify(errors.New("inspecting table: boom"))
	assert.Equal(t, ErrCodeInspection, code)
	assert.Equal(t, ExitInspection, exit)

	assert.NotEqual(t, ExitOK, ExitConfig)
}
