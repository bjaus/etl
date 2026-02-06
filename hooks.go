package etl

import "context"

// Filter excludes records before transformation. Implement this interface when
// you need to skip records based on their content without incurring the cost of
// transformation.
//
// Filter runs in the extract stage, before any concurrency. This makes it the
// most efficient place to drop records because filtered records never enter the
// transform or load stages and never consume worker capacity.
//
// Use Filter when you have:
//   - Soft-deleted records that should be ignored
//   - Records outside a target date range
//   - Inactive or disabled entities
//   - Any condition that can be evaluated from the source record alone
//
// Example:
//
//	func (j *MyJob) Include(src Source) bool {
//	    return !src.Deleted && src.UpdatedAt.After(j.since)
//	}
//
// If you need access to context or want to produce errors, consider filtering
// inside Transformer or Expander instead. Returning an empty slice from Expander
// has the same effect, but the record still passes through the transform stage.
//
// Filter interacts with Checkpointer: in epoch mode, filtered records still
// advance the cursor so that resuming from a checkpoint does not re-process
// records that were already seen and skipped.
type Filter[S any] interface {
	// Include returns true if the record should be processed.
	// Returning false skips the record before it reaches the transform stage.
	Include(src S) bool
}

// ErrorHandler customizes error handling per pipeline stage. Without an
// ErrorHandler, the pipeline stops on the first error in any stage.
//
// Implement this interface when you want to:
//   - Log errors and continue processing (return ActionSkip)
//   - Apply different strategies per stage (e.g., skip extract errors, fail on load errors)
//   - Track error counts for alerting or metrics
//
// Common patterns:
//
//	// Log-and-skip for transform, fail for load (data integrity)
//	func (j *MyJob) OnError(ctx context.Context, stage etl.Stage, err error) etl.Action {
//	    switch stage {
//	    case etl.StageExtract:
//	        slog.Warn("skipping malformed record", "error", err)
//	        return etl.ActionSkip
//	    case etl.StageTransform:
//	        slog.Error("transform error", "error", err)
//	        return etl.ActionSkip
//	    case etl.StageLoad:
//	        return etl.ActionFail
//	    }
//	    return etl.ActionFail
//	}
//
// Skipped errors still increment Stats.Errors. The err parameter passed to
// Stopper.Stop only contains the fatal error that caused the pipeline to fail
// (i.e., when OnError returned ActionFail or when no ErrorHandler is present).
type ErrorHandler interface {
	// OnError is called when an error occurs during any stage.
	// Return ActionSkip to continue processing, ActionFail to stop the pipeline.
	OnError(ctx context.Context, stage Stage, err error) Action
}

// Starter is called before pipeline execution begins. Implement this interface
// when you need to perform setup work or enrich the context before extraction
// starts.
//
// Use Starter for:
//   - Adding values to the context (request IDs, trace spans, logger fields)
//   - Recording the pipeline start time for elapsed-time metrics
//   - Acquiring resources that must be held for the pipeline's lifetime
//   - Logging the start of a pipeline run
//
// The context returned by Start is propagated to all pipeline stages and to
// Stopper.Stop. This makes it the right place to attach tracing spans or
// cancellation signals.
//
// Example:
//
//	func (j *MyJob) Start(ctx context.Context) context.Context {
//	    j.startedAt = time.Now()
//	    slog.InfoContext(ctx, "pipeline starting")
//	    return ctx
//	}
//
// Start is called exactly once, before the first call to Extract.
type Starter interface {
	// Start is called before extraction begins.
	// The returned context is used for the entire pipeline.
	Start(ctx context.Context) context.Context
}

// Stopper is called after pipeline execution completes, regardless of whether
// the pipeline succeeded, failed, or was shut down gracefully. Implement this
// interface for cleanup, final logging, or metrics reporting.
//
// Use Stopper for:
//   - Logging final stats and elapsed time
//   - Reporting success/failure metrics to an observability system
//   - Releasing resources acquired in Starter
//   - Sending completion notifications
//
// The ctx passed to Stop is derived from the pipeline's drain context, which
// remains valid even during graceful shutdown. This allows Stop to perform
// cleanup operations (database writes, API calls) after the parent context
// has been cancelled.
//
// The err parameter is the same error value returned by Run: the unrecoverable
// error that caused Run to fail (no ErrorHandler, or ErrorHandler returned
// ActionFail). Errors handled with ActionSkip do not appear in err, even though
// they increment stats.Errors while the pipeline continues processing.
//
// Example:
//
//	func (j *MyJob) Stop(ctx context.Context, stats *etl.Stats, err error) {
//	    elapsed := time.Since(j.startedAt)
//	    if err != nil {
//	        slog.ErrorContext(ctx, "pipeline failed", "error", err, "stats", stats, "elapsed", elapsed)
//	    } else {
//	        slog.InfoContext(ctx, "pipeline complete", "stats", stats, "elapsed", elapsed)
//	    }
//	}
//
// Stop is called exactly once, after the pipeline Run method returns.
type Stopper interface {
	// Stop is called exactly once, after the pipeline Run method returns.
	Stop(ctx context.Context, stats *Stats, err error)
}
