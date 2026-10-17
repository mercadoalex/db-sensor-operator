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

package dashboard

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mercadoalex/db-sensor-operator/internal/events"
)

// tableOld is a shared table-label literal used across the pruning tests.
const tableOld = "old"

func TestRingBufferNewestFirst(t *testing.T) {
	buf := NewRingBuffer(0, func() time.Time { return time.Unix(0, 0) })
	for i := range 3 {
		buf.Add(events.CDCEvent{Op: "c", DB: "pg", Table: strconv.Itoa(i)})
	}
	got := buf.Recent()
	require.Len(t, got, 3)
	// Newest-first: last appended (table "2") comes first.
	assert.Equal(t, "2", got[0].Table)
	assert.Equal(t, "1", got[1].Table)
	assert.Equal(t, "0", got[2].Table)
}

func TestRingBufferEmpty(t *testing.T) {
	buf := NewRingBuffer(0, nil)
	assert.Empty(t, buf.Recent())
}

func TestRingBufferCapacityOverflow(t *testing.T) {
	buf := NewRingBuffer(0, func() time.Time { return time.Unix(0, 0) })
	// Push one more than capacity.
	for i := range MaxEvents + 1 {
		buf.Add(events.CDCEvent{Op: "c", DB: "pg", Table: strconv.Itoa(i)})
	}
	got := buf.Recent()
	require.Len(t, got, MaxEvents)
	// Newest (last appended) is index MaxEvents; the very first (table "0") was
	// overwritten and must be gone.
	assert.Equal(t, strconv.Itoa(MaxEvents), got[0].Table)
	for _, ev := range got {
		assert.NotEqual(t, "0", ev.Table, "oldest event should have been dropped")
	}
	// Oldest retained is table "1".
	assert.Equal(t, "1", got[len(got)-1].Table)
}

func TestRingBufferTimePruning(t *testing.T) {
	now := time.Unix(1000, 0)
	clock := func() time.Time { return now }
	buf := NewRingBuffer(10*time.Second, clock)

	// Record an old event, then advance the clock past the window.
	buf.Add(events.CDCEvent{Op: "c", DB: "pg", Table: tableOld})
	now = now.Add(20 * time.Second)
	// A fresh event recorded at the new now.
	buf.Add(events.CDCEvent{Op: "c", DB: "pg", Table: "fresh"})

	got := buf.Recent()
	require.Len(t, got, 1)
	assert.Equal(t, "fresh", got[0].Table)
}

func TestRingBufferZeroRetentionDisablesPruning(t *testing.T) {
	now := time.Unix(1000, 0)
	clock := func() time.Time { return now }
	buf := NewRingBuffer(0, clock)

	buf.Add(events.CDCEvent{Op: "c", DB: "pg", Table: tableOld})
	now = now.Add(100 * time.Hour)
	buf.Add(events.CDCEvent{Op: "c", DB: "pg", Table: "new"})

	got := buf.Recent()
	assert.Len(t, got, 2, "zero retention must keep everything up to capacity")
}

func TestRingBufferSetRetention(t *testing.T) {
	now := time.Unix(1000, 0)
	clock := func() time.Time { return now }
	buf := NewRingBuffer(0, clock)

	buf.Add(events.CDCEvent{Op: "c", DB: "pg", Table: tableOld})
	now = now.Add(30 * time.Second)
	buf.Add(events.CDCEvent{Op: "c", DB: "pg", Table: "new"})

	// With no retention both are visible.
	assert.Len(t, buf.Recent(), 2)

	// Tighten the window to 10s: the old event falls out.
	buf.SetRetention(10 * time.Second)
	got := buf.Recent()
	require.Len(t, got, 1)
	assert.Equal(t, "new", got[0].Table)
}

func TestRingBufferConcurrentAddAndRecent(t *testing.T) {
	buf := NewRingBuffer(0, nil)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writer.
	wg.Go(func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
				buf.Add(events.CDCEvent{Op: "c", DB: "pg", Table: strconv.Itoa(i)})
			}
		}
	})

	// Reader.
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				_ = buf.Recent()
			}
		}
	})

	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
}
