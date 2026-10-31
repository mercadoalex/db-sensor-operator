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

// This file wires the pipeline together. The Pipeline is the handler a Consumer
// calls for each parsed event; it fans the event out to the metrics Exporter and
// the health LagTracker (DECISION 2 wiring).
//
// Import direction (acyclic): internal/metrics imports internal/events for the
// CDCEvent type (Exporter.OnEvent(events.CDCEvent)). To keep that one-way,
// internal/events must NOT import internal/metrics. The Pipeline therefore refers
// to the Exporter and the LagTracker through the tiny local eventSink and
// lagSink interfaces below, which *metrics.Exporter and *health.LagTracker
// satisfy structurally — no import of those packages from here, so go build is
// clean. cmd/main.go (which may import all three) builds the Pipeline with the
// concrete types.
package events

// eventSink is satisfied by *metrics.Exporter. Declared here so the Pipeline does
// not import internal/metrics (which imports this package), avoiding a cycle.
type eventSink interface {
	OnEvent(ev CDCEvent)
}

// lagSink is satisfied by *health.LagTracker.
type lagSink interface {
	OnEvent(tsMs int64)
}

// recorder is satisfied by *dashboard.RingBuffer. Declared here (not imported)
// so internal/events does not import internal/dashboard, which imports this
// package for the CDCEvent type — keeping the import graph acyclic.
type recorder interface {
	Add(ev CDCEvent)
}

// Pipeline fans a CDC event out to the observability components. Exporter is the
// metrics sink; Lag is the health lag tracker and Recorder is the dashboard ring
// buffer. Lag and Recorder are optional (nil-safe) so the metrics-only path works
// without them.
//
// The fields are interface-typed only to keep the import graph acyclic; callers
// assign the concrete *metrics.Exporter, *health.LagTracker, and
// *dashboard.RingBuffer.
type Pipeline struct {
	Exporter eventSink
	Lag      lagSink
	Recorder recorder
}

// Handle is the Consumer handler: it feeds the event to the Exporter (metrics),
// and, when present, to the LagTracker (health) and the dashboard RingBuffer
// (recent-events history). Recording is always on when a Recorder is set, so the
// dashboard shows history as soon as it is enabled. Pass p.Handle as the handler
// to a Consumer's Start.
//
// Snapshot events (existing rows read during a snapshot) are dropped: they carry
// the timestamp of the read, so they would make a healthy sensor look laggy, and
// they are not mutations, so they belong in no counter or history. See ADR-014.
func (p *Pipeline) Handle(ev CDCEvent) {
	if ev.Snapshot {
		return
	}
	if p.Exporter != nil {
		p.Exporter.OnEvent(ev)
	}
	if p.Lag != nil {
		p.Lag.OnEvent(ev.TsMs)
	}
	if p.Recorder != nil {
		p.Recorder.Add(ev)
	}
}
