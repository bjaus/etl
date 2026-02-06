// Package etl provides a flexible Extract-Transform-Load pipeline framework.
//
// The ETL package uses an interface-based API where your job type implements only
// the interfaces it needs. The pipeline auto-detects implemented interfaces and
// configures itself accordingly. Runtime configuration overrides are also available
// via method chaining.
//
// # Quick Start
//
// Implement the required Job interface:
//
//	type MyJob struct {
//	    db *sql.DB
//	}
//
//	func (j *MyJob) Extract(ctx context.Context, cursor *int64) iter.Seq2[Source, error] {
//	    return func(yield func(Source, error) bool) {
//	        rows, err := j.db.QueryContext(ctx, "SELECT id, name FROM source WHERE id > $1", lo.FromPtr(cursor))
//	        if err != nil {
//	            yield(Source{}, err)
//	            return
//	        }
//	        defer rows.Close()
//	        for rows.Next() {
//	            var r Source
//	            if err := rows.Scan(&r.ID, &r.Name); err != nil {
//	                if !yield(Source{}, err) { return }
//	                continue
//	            }
//	            if !yield(r, nil) { return }
//	        }
//	    }
//	}
//
//	func (j *MyJob) Transform(ctx context.Context, src Source) (Target, error) {
//	    return Target{ID: src.ID, Name: strings.ToUpper(src.Name)}, nil
//	}
//
//	func (j *MyJob) Load(ctx context.Context, batch []Target) error {
//	    // UPSERT batch to destination
//	    return j.db.BulkUpsert(ctx, batch)
//	}
//
//	// Run the pipeline
//	err := etl.New[Source, Target, int64](&MyJob{db: db}).Run(ctx)
//
// # Interface-Based Design
//
// The pipeline auto-detects optional interfaces. Just implement what you need:
//
//	// Add filtering by implementing Filter[S]
//	func (j *MyJob) Include(src Source) bool {
//	    return src.Active
//	}
//
//	// Add error handling by implementing ErrorHandler
//	func (j *MyJob) OnError(ctx context.Context, stage etl.Stage, err error) etl.Action {
//	    slog.Error("error in pipeline", "stage", stage, "error", err)
//	    return etl.ActionSkip // or etl.ActionFail to stop
//	}
//
//	// Add progress tracking by implementing ProgressReporter
//	func (j *MyJob) ReportInterval() int { return 10000 }
//	func (j *MyJob) OnProgress(ctx context.Context, stats *etl.Stats) {
//	    slog.Info("progress", "loaded", stats.Loaded())
//	}
//
// # Checkpointing for Resumability
//
// Implement Checkpointer[S, C] for checkpoint-based resumability:
//
//	func (j *MyJob) CheckpointInterval() int { return 5000 }
//	func (j *MyJob) Cursor(src Source) int64 { return src.ID }
//
//	func (j *MyJob) LoadCheckpoint(ctx context.Context) (cur *int64, stats *etl.Stats, err error) {
//	    var c sql.NullInt64
//	    var extracted, filtered, transformed, loaded, errCount int64
//	    err = j.db.QuerySourceContext(ctx,
//	        `SELECT cursor, extracted, filtered, transformed, loaded, errors
//	         FROM checkpoints WHERE job_id = $1`, j.jobID,
//	    ).Scan(&c, &extracted, &filtered, &transformed, &loaded, &errCount)
//	    if errors.Is(err, sql.ErrNoSources) || !c.Valid {
//	        return // Fresh start
//	    }
//	    if err != nil {
//	        return
//	    }
//	    return &c.Int64, etl.NewStats(extracted, filtered, transformed, loaded, errCount), nil
//	}
//
//	func (j *MyJob) SaveCheckpoint(ctx context.Context, cursor int64, stats *etl.Stats) error {
//	    _, err := j.db.ExecContext(ctx,
//	        `INSERT INTO checkpoints (job_id, cursor, extracted, filtered, transformed, loaded, errors, updated_at)
//	         VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
//	         ON CONFLICT (job_id) DO UPDATE SET
//	             cursor = $2, extracted = $3, filtered = $4, transformed = $5, loaded = $6, errors = $7, updated_at = NOW()`,
//	        j.jobID, cursor, stats.Extracted(), stats.Filtered(), stats.Transformed(), stats.Loaded(), stats.Errors(),
//	    )
//	    return err
//	}
//
//	func (j *MyJob) ClearCheckpoint(ctx context.Context) error {
//	    _, err := j.db.ExecContext(ctx,
//	        "DELETE FROM checkpoints WHERE job_id = $1", j.jobID,
//	    )
//	    return err
//	}
//
// When checkpointing is enabled, the pipeline:
//   - Processes records in epochs of CheckpointInterval() records
//   - Waits for all transforms and loads in an epoch to complete
//   - Saves the checkpoint cursor after each successful epoch
//   - On restart, resumes from the last saved cursor
//   - Clears the checkpoint after successful completion
//
// # Execution Modes
//
// The pipeline operates in one of two modes:
//
// Streaming Mode (default): When Checkpointer is not implemented, records flow
// through the pipeline in a streaming fashion with concurrent stages.
//
// Epoch Mode: When Checkpointer is implemented, records are processed in epochs.
// Each epoch collects CheckpointInterval() records, processes them with concurrency,
// then saves a checkpoint before starting the next epoch.
//
// # Configuration
//
// Every configuration knob follows the same pattern: a WithXxx builder method and
// a matching Xxx interface with an Xxx() method. The builder always takes priority.
//
// Configure the pipeline with method chaining:
//
//	err := etl.New[Source, Target, int64](&MyJob{}).
//	    WithTransformWorkers(4).       // Concurrent transform workers
//	    WithLoadWorkers(2).            // Concurrent load workers
//	    WithLoadBatchSize(100).        // Targets per load batch
//	    WithReportInterval(5000).      // Progress interval override
//	    WithDrainTimeout(time.Minute). // Graceful shutdown timeout
//	    Run(ctx)
//
// Or implement the corresponding interfaces in your job:
//
//	func (j *MyJob) TransformWorkers() int       { return 4 }
//	func (j *MyJob) LoadWorkers() int             { return 2 }
//	func (j *MyJob) LoadBatchSize() int            { return 100 }
//	func (j *MyJob) DrainTimeout() time.Duration   { return time.Minute }
//
// Configuration priority (highest to lowest):
//  1. WithXxx() method overrides
//  2. Interface implementations
//  3. Default values
//
// # Batching
//
// By default the pipeline batches records by count using LoadBatchSize (default 100).
// For custom batching logic, implement Batcher[T] on your job:
//
//	func (j *MyJob) Batch(items []Target) [][]Target {
//	    // Group by category
//	    byCategory := make(map[string][]Target)
//	    for _, item := range items {
//	        byCategory[item.Category] = append(byCategory[item.Category], item)
//	    }
//	    var batches [][]Target
//	    for _, batch := range byCategory {
//	        batches = append(batches, batch)
//	    }
//	    return batches
//	}
//
// The package also provides ready-made batchers that can be used inside a Batch
// method implementation:
//
//	// Fixed-size batches (same as default behavior)
//	func (j *MyJob) Batch(items []Target) [][]Target {
//	    return etl.SizeBatcher[Target](500).Batch(items)
//	}
//
//	// Group by a field value so related records are loaded together
//	func (j *MyJob) Batch(items []Target) [][]Target {
//	    return etl.GroupByField(func(t Target) string { return t.Category }).Batch(items)
//	}
//
//	// Group by field, then cap each group at 50
//	func (j *MyJob) Batch(items []Target) [][]Target {
//	    return etl.GroupByFieldWithSizeLimit(
//	        func(t Target) string { return t.Category },
//	        50,
//	    ).Batch(items)
//	}
//
//	// Batch by cumulative weight (e.g., SQL parameter limits).
//	// Each row uses 5 INSERT parameters; Postgres allows 65535 per statement.
//	func (j *MyJob) Batch(items []Target) [][]Target {
//	    return etl.WeightedBatcher(func(t Target) int { return 5 }, 65535).Batch(items)
//	}
//
//	// Hash-stripe for parallel load workers without lock contention.
//	// Items with the same key always land in the same stripe; batches are interleaved
//	// across stripes so concurrent workers naturally avoid contending on the same rows.
//	func (j *MyJob) Batch(items []Target) [][]Target {
//	    return etl.StripeBatcher(
//	        func(t Target) string { return t.Category },
//	        4,   // numStripes (match WithLoadWorkers)
//	        500, // maxBatchSize
//	    ).Batch(items)
//	}
//
//	// Compose multiple strategies: group by category, then cap at 10 KB per batch
//	func (j *MyJob) Batch(items []Target) [][]Target {
//	    return etl.CombineBatchers(
//	        etl.GroupByField(func(t Target) string { return t.Category }),
//	        etl.WeightedBatcher(func(t Target) int { return t.Size }, 10*1024),
//	    ).Batch(items)
//	}
//
//	// Send everything in a single batch (disable batching entirely)
//	func (j *MyJob) Batch(items []Target) [][]Target {
//	    return etl.NoBatcher[Target]().Batch(items)
//	}
//
// # Lifecycle Hooks
//
// Implement Starter and/or Stopper for setup and cleanup:
//
//	func (j *MyJob) Start(ctx context.Context) context.Context {
//	    j.startedAt = time.Now()
//	    slog.Info("starting pipeline")
//	    return ctx
//	}
//
//	func (j *MyJob) Stop(ctx context.Context, stats *etl.Stats, err error) {
//	    elapsed := time.Since(j.startedAt)
//	    rate := float64(stats.Loaded()) / elapsed.Seconds()
//	    if err != nil {
//	        slog.Error("pipeline failed", "error", err, "processed", stats.Loaded())
//	    } else {
//	        slog.Info("pipeline complete", "loaded", stats.Loaded(), "elapsed", elapsed, "rate", rate)
//	    }
//	}
//
// # Transform Patterns
//
// One-to-one transformation (implement Transformer[S, T]):
//
//	func (j *MyJob) Transform(ctx context.Context, src Source) (Target, error) {
//	    return Target{ID: src.ID, Name: src.Name}, nil
//	}
//
// One-to-many transformation (implement Expander[S, T]):
//
//	func (j *MyJob) Expand(ctx context.Context, src Source) ([]Target, error) {
//	    return src.Items, nil // One source row produces multiple output records
//	}
//
// # Filtering Targets
//
// Use the Filter[S] interface for pre-transform filtering:
//
//	func (j *MyJob) Include(src Source) bool {
//	    return !src.Deleted // Skip deleted records before transform
//	}
//
// This is more efficient than filtering in Transform since it skips the
// transformation step entirely.
//
// # Error Handling
//
// Without ErrorHandler, the pipeline stops on the first error.
//
// With ErrorHandler, you control error behavior:
//
//	func (j *MyJob) OnError(ctx context.Context, stage etl.Stage, err error) etl.Action {
//	    switch stage {
//	    case etl.StageExtract:
//	        // Skip malformed records
//	        slog.Warn("skipping record", "error", err)
//	        return etl.ActionSkip
//	    case etl.StageTransform:
//	        // Log and skip transform errors
//	        slog.Error("transform error", "error", err)
//	        return etl.ActionSkip
//	    case etl.StageLoad:
//	        // Fail on load errors (data integrity)
//	        return etl.ActionFail
//	    }
//	    return etl.ActionFail
//	}
//
// # Best Practices
//
// Load operations should be idempotent (use UPSERT) to handle:
//   - Pipeline restarts after partial completion
//   - Re-processing of records near checkpoint boundaries
//
// Use generous CheckpointInterval() values (1000-10000) to balance:
//   - Recovery granularity (smaller = less re-processing on restart)
//   - Checkpoint overhead (larger = less database writes)
//
// Match LoadWorkers() to your database connection pool size.
//
// For graceful shutdown on SIGINT/SIGTERM, set up signal handling before calling Run:
//
//	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
//	defer cancel()
//	err := etl.New[Source, Target, int64](&MyJob{}).Run(ctx)
package etl
