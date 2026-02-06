package etl

import "context"

// ReportInterval controls how often progress is reported, measured in records
// loaded. This interface can be implemented independently of ProgressReporter
// when you want to set the interval via the job struct rather than the builder.
//
// The value can be overridden at runtime via WithReportInterval, which takes
// precedence over this interface. If neither is set, DefaultReportInterval
// (10,000 records) is used.
//
// This interface is embedded in ProgressReporter, so implementing
// ProgressReporter automatically satisfies ReportInterval.
//
// Example:
//
//	func (j *MyJob) ReportInterval() int { return 5000 }
type ReportInterval interface {
	// ReportInterval returns how often to call OnProgress (in records loaded).
	ReportInterval() int
}

// ProgressReporter receives periodic progress updates during pipeline
// execution. Implement this interface when you want to log throughput, emit
// metrics, or update an external dashboard while the pipeline is running.
//
// OnProgress is called each time the cumulative loaded count crosses a
// ReportInterval boundary. In streaming mode this happens inside load workers;
// in epoch mode it happens during batch loading within each epoch.
//
// The Stats snapshot passed to OnProgress is safe to read concurrently.
// Avoid performing blocking I/O inside OnProgress since it runs on a load
// worker goroutine.
//
// Use ProgressReporter when:
//   - You want periodic log lines showing loaded/error counts
//   - You need to push throughput metrics to Prometheus, Datadog, etc.
//   - You want a heartbeat to detect stalled pipelines
//
// Example:
//
//	func (j *MyJob) ReportInterval() int { return 10000 }
//
//	func (j *MyJob) OnProgress(ctx context.Context, stats *etl.Stats) {
//	    slog.InfoContext(ctx, "progress",
//	        "extracted", stats.Extracted(),
//	        "loaded", stats.Loaded(),
//	        "errors", stats.Errors(),
//	    )
//	}
//
// To override the interval at runtime without changing the job struct, use
// WithReportInterval on the pipeline builder instead.
type ProgressReporter interface {
	ReportInterval

	// OnProgress is called periodically during execution.
	OnProgress(ctx context.Context, stats *Stats)
}
