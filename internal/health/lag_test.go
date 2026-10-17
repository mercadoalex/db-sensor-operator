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

package health

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLagTrackerZeroBeforeAnyEvent(t *testing.T) {
	lt := NewLagTracker()
	assert.Equal(t, time.Duration(0), lt.GetLag())
	assert.True(t, lt.LastEvent().IsZero())
}

func TestLagTrackerGetLag(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	lt := NewLagTrackerWithClock(func() time.Time { return now })

	// Event produced 42 seconds ago.
	eventTs := now.Add(-42 * time.Second)
	lt.OnEvent(eventTs.UnixMilli())

	assert.InDelta(t, (42 * time.Second).Seconds(), lt.GetLag().Seconds(), 0.001)
	assert.Equal(t, eventTs.UnixMilli(), lt.LastEvent().UnixMilli())
}

func TestLagTrackerLatestEventWins(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	lt := NewLagTrackerWithClock(func() time.Time { return now })

	lt.OnEvent(now.Add(-100 * time.Second).UnixMilli())
	lt.OnEvent(now.Add(-5 * time.Second).UnixMilli())

	assert.InDelta(t, (5 * time.Second).Seconds(), lt.GetLag().Seconds(), 0.001)
}

func TestLagTrackerConcurrent(t *testing.T) {
	lt := NewLagTracker()
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(2)
		go func(n int) {
			defer wg.Done()
			lt.OnEvent(time.Now().Add(-time.Duration(n) * time.Second).UnixMilli())
		}(i)
		go func() {
			defer wg.Done()
			_ = lt.GetLag()
			_ = lt.LastEvent()
		}()
	}
	wg.Wait()
}
