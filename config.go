package etl

import "time"

// Default configuration values.
const (
	DefaultTransformWorkers = 1
	DefaultLoadWorkers      = 1
	DefaultLoadBatchSize    = 100
	DefaultReportInterval   = 10000
	DefaultDrainTimeout     = 5 * time.Minute
)

// TransformWorkers controls worker parallelism for the transform stage.
// Implement this interface to set the concurrency level from the job struct
// rather than the pipeline builder.
//
// The value can be overridden at runtime via WithTransformWorkers, which takes
// precedence. If neither is set, DefaultTransformWorkers (1) is used.
//
// Tuning guidance:
//   - CPU-bound transforms (parsing, hashing, serialization): set to runtime.NumCPU()
//   - I/O-bound transforms (HTTP calls, cache lookups): set higher (10-50) depending
//     on the external service's capacity
//   - Pure field mapping with no I/O: 1 worker is usually sufficient since the
//     overhead of goroutine coordination outweighs the parallelism benefit
//
// Example:
//
//	func (j *MyJob) TransformWorkers() int { return runtime.NumCPU() }
type TransformWorkers interface {
	// TransformWorkers returns the number of concurrent transform workers.
	TransformWorkers() int
}

// LoadWorkers controls worker parallelism for the load stage. Implement this
// interface to set the concurrency level from the job struct rather than the
// pipeline builder.
//
// The value can be overridden at runtime via WithLoadWorkers, which takes
// precedence. If neither is set, DefaultLoadWorkers (1) is used.
//
// Tuning guidance:
//   - Match to your database connection pool size to avoid pool exhaustion
//   - For batch INSERTs/UPSERTs, 2-4 workers often saturates a single database
//   - Consider using StripeBatcher with matching stripe count to avoid lock
//     contention when multiple workers write to the same table
//
// Example:
//
//	func (j *MyJob) LoadWorkers() int { return 4 }
type LoadWorkers interface {
	// LoadWorkers returns the number of concurrent load workers.
	LoadWorkers() int
}

// LoadBatchSize controls the number of records batched together before calling
// Load. Implement this interface to set the batch size from the job struct
// rather than the pipeline builder.
//
// The value can be overridden at runtime via WithLoadBatchSize, which takes
// precedence. If neither is set, DefaultLoadBatchSize (100) is used.
//
// This value is used as the default batch size when no custom Batcher is
// implemented. When a custom Batcher is present, this value is ignored in
// favor of the Batcher's own logic.
//
// Tuning guidance:
//   - For SQL INSERTs: balance between fewer round-trips (larger batches) and
//     staying within parameter limits (e.g., PostgreSQL's 65535 param limit)
//   - For API calls: match the external API's preferred batch size
//   - For file writes: larger batches reduce I/O overhead
//
// Example:
//
//	func (j *MyJob) LoadBatchSize() int { return 500 }
type LoadBatchSize interface {
	// LoadBatchSize returns the number of records to batch before loading.
	LoadBatchSize() int
}

// DrainTimeout controls graceful shutdown behavior. When the parent context is
// cancelled (e.g., SIGTERM), the pipeline:
//
//  1. Stops extracting new records immediately
//  2. Allows in-flight transform/load operations to complete within the timeout
//  3. If operations complete within timeout: exits cleanly with nil error
//  4. If timeout expires: forces abort with error
//
// Implement this interface to set the timeout from the job struct rather than
// the pipeline builder.
//
// The value can be overridden at runtime via WithDrainTimeout, which takes
// precedence. If neither is set, DefaultDrainTimeout (5 minutes) is used.
//
// Set to 0 to disable graceful shutdown entirely (immediate abort on context
// cancellation). Negative values passed to WithDrainTimeout are ignored.
//
// When to customize:
//   - Jobs with fast loads (< 1s per batch): a short timeout (30s) is sufficient
//   - Jobs with slow external API calls: increase to match worst-case latency
//   - Jobs where partial progress is not useful: set to 0 to fail fast
//
// In checkpoint mode, a successful drain saves the checkpoint before exiting,
// so the next restart picks up exactly where the shutdown left off.
//
// Example:
//
//	func (j *MyJob) DrainTimeout() time.Duration { return 30 * time.Second }
type DrainTimeout interface {
	// DrainTimeout returns the maximum time to wait for in-flight operations
	// to complete after the parent context is cancelled.
	// A zero value disables graceful shutdown (immediate abort).
	DrainTimeout() time.Duration
}

// resolveTransformWorkers returns the effective transform worker count.
// Priority: WithTransformWorkers > TransformWorkers interface > DefaultTransformWorkers.
func (p *Pipeline[S, T, C]) resolveTransformWorkers() int {
	if p.transformWorkerCount != nil {
		return *p.transformWorkerCount
	}
	if p.transformWorkers != nil {
		return p.transformWorkers.TransformWorkers()
	}
	return DefaultTransformWorkers
}

// resolveLoadWorkers returns the effective load worker count.
// Priority: WithLoadWorkers > LoadWorkers interface > DefaultLoadWorkers.
func (p *Pipeline[S, T, C]) resolveLoadWorkers() int {
	if p.loadWorkerCount != nil {
		return *p.loadWorkerCount
	}
	if p.loadWorkers != nil {
		return p.loadWorkers.LoadWorkers()
	}
	return DefaultLoadWorkers
}

// resolveLoadBatchSize returns the effective load batch size.
// Priority: WithLoadBatchSize > LoadBatchSize interface > DefaultLoadBatchSize.
func (p *Pipeline[S, T, C]) resolveLoadBatchSize() int {
	if p.batchSize != nil {
		return *p.batchSize
	}
	if p.loadBatchSizeIface != nil {
		return p.loadBatchSizeIface.LoadBatchSize()
	}
	return DefaultLoadBatchSize
}

// resolveReportInterval returns the effective report interval.
// Priority: WithReportInterval > ReportInterval interface > DefaultReportInterval.
func (p *Pipeline[S, T, C]) resolveReportInterval() int {
	if p.reportInterval != nil {
		return *p.reportInterval
	}
	if p.reportIntervalIface != nil {
		return p.reportIntervalIface.ReportInterval()
	}
	return DefaultReportInterval
}

// resolveBatcher returns the effective batcher.
// Uses the job's Batcher if implemented, otherwise falls back to SizeBatcher
// with the resolved load batch size.
func (p *Pipeline[S, T, C]) resolveBatcher() Batcher[T] {
	if p.batcher != nil {
		return p.batcher
	}
	return SizeBatcher[T](p.resolveLoadBatchSize())
}

// resolveDrainTimeout returns the effective drain timeout.
// Priority: WithDrainTimeout > DrainTimeout interface > DefaultDrainTimeout.
func (p *Pipeline[S, T, C]) resolveDrainTimeout() time.Duration {
	if p.drainTimeout != nil {
		return *p.drainTimeout
	}
	if p.drainTimeoutIface != nil {
		return p.drainTimeoutIface.DrainTimeout()
	}
	return DefaultDrainTimeout
}
