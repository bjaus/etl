package etl

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"
)

// Checkpointer enables checkpoint-based resumability for long-running ETL jobs.
//
// When implemented, the pipeline switches from streaming mode to epoch-based
// mode. Records are processed in epochs of CheckpointInterval() records:
//
//  1. Extract up to CheckpointInterval() records
//  2. Transform and load them with configured concurrency
//  3. Save a checkpoint (cursor + stats) after the epoch completes
//  4. Repeat from step 1 until the source is exhausted
//  5. Clear the checkpoint on successful completion
//
// On restart, LoadCheckpoint returns the saved cursor and stats so the pipeline
// resumes from where it left off. Extract receives the cursor to skip already-
// processed records.
//
// Use Checkpointer when:
//   - The dataset is large enough that re-processing from scratch is expensive
//   - The job may be interrupted (deploys, spot instances, timeouts)
//   - You need progress durability across process restarts
//
// Idempotency requirement: because records near a checkpoint boundary may be
// re-processed on restart, the Load implementation should be idempotent (use
// UPSERT rather than INSERT). Cursor values must increase monotonically with
// source ordering so that resuming from a cursor skips the right records.
//
// Choosing CheckpointInterval:
//   - Smaller values (500-2000): finer recovery granularity, more checkpoint writes
//   - Larger values (5000-10000): fewer checkpoint writes, more re-processing on restart
//   - Match to your source's natural page size when possible
//
// Interaction with graceful shutdown: when the parent context is cancelled,
// the current epoch is allowed to complete (within DrainTimeout), and the
// checkpoint is saved before the pipeline exits. This means the next restart
// will not re-process the completed epoch.
//
// Example:
//
//	func (j *MyJob) CheckpointInterval() int { return 5000 }
//
//	func (j *MyJob) Cursor(src Source) int64 { return src.ID }
//
//	func (j *MyJob) LoadCheckpoint(ctx context.Context) (*int64, *etl.Stats, error) {
//	    // Return (nil, nil, nil) for a fresh start
//	    row := j.db.QueryRowContext(ctx, "SELECT cursor, stats FROM checkpoints WHERE job = $1", j.id)
//	    var cursor int64
//	    var statsJSON []byte
//	    if err := row.Scan(&cursor, &statsJSON); err != nil {
//	        if errors.Is(err, sql.ErrNoRows) { return nil, nil, nil }
//	        return nil, nil, err
//	    }
//	    var stats etl.Stats
//	    if err := stats.UnmarshalJSON(statsJSON); err != nil { return nil, nil, err }
//	    return &cursor, &stats, nil
//	}
//
//	func (j *MyJob) SaveCheckpoint(ctx context.Context, cursor int64, stats *etl.Stats) error {
//	    data, _ := stats.MarshalJSON()
//	    _, err := j.db.ExecContext(ctx,
//	        `INSERT INTO checkpoints (job, cursor, stats) VALUES ($1, $2, $3)
//	         ON CONFLICT (job) DO UPDATE SET cursor = $2, stats = $3`, j.id, cursor, data)
//	    return err
//	}
//
//	func (j *MyJob) ClearCheckpoint(ctx context.Context) error {
//	    _, err := j.db.ExecContext(ctx, "DELETE FROM checkpoints WHERE job = $1", j.id)
//	    return err
//	}
//
//nolint:dupword // code example with multiple nil returns
type Checkpointer[S any, C comparable] interface {
	// CheckpointInterval returns the number of records to process per epoch.
	CheckpointInterval() int

	// Cursor extracts a checkpoint cursor from a source record.
	// The cursor must increase monotonically with source ordering.
	Cursor(src S) C

	// LoadCheckpoint retrieves the last saved cursor and stats.
	// Return (nil, nil, nil) if no checkpoint exists (fresh start).
	LoadCheckpoint(ctx context.Context) (cursor *C, stats *Stats, err error)

	// SaveCheckpoint persists cursor and stats.
	// Called by the pipeline after each epoch completes successfully.
	SaveCheckpoint(ctx context.Context, cursor C, stats *Stats) error

	// ClearCheckpoint removes the saved checkpoint.
	// Called by the pipeline after successful completion.
	ClearCheckpoint(ctx context.Context) error
}

// runWithCheckpoints executes the pipeline in epoch-based mode with checkpointing.
// Returns (drainedSuccessfully, error) where drainedSuccessfully is true if:
// - The parent context was cancelled AND
// - The current epoch completed and checkpoint was saved
func (p *Pipeline[S, T, C]) runWithCheckpoints(ctx, drainCtx context.Context, cursor *C, stats *Stats) (bool, error) {
	for {
		// Check for graceful shutdown before starting new epoch
		select {
		case <-ctx.Done():
			// Parent cancelled before starting new epoch - nothing in flight, clean exit
			return true, nil
		default:
		}

		// Process one epoch using drainCtx so in-flight work can complete
		lastCursor, err := p.processEpoch(ctx, drainCtx, cursor, stats)
		if err != nil {
			return false, err
		}

		// No more records extracted - we're done
		if lastCursor == nil {
			return true, nil
		}

		// Check if parent context was cancelled DURING the epoch
		select {
		case <-ctx.Done():
			// Graceful shutdown - epoch completed, save checkpoint and exit
			if err := p.checkpoint.SaveCheckpoint(drainCtx, *lastCursor, stats); err != nil {
				return false, fmt.Errorf("failed to save checkpoint during shutdown: %w", err)
			}
			return true, nil
		default:
		}

		// Normal operation - save checkpoint and continue
		if err := p.checkpoint.SaveCheckpoint(ctx, *lastCursor, stats); err != nil {
			return false, fmt.Errorf("failed to save checkpoint: %w", err)
		}

		cursor = lastCursor
	}
}

// processEpoch extracts and processes up to CheckpointSize() records.
// ctx is checked for shutdown signal (stop extracting), drainCtx is used for transform/load.
// Returns the cursor to advance to and any error.
// If all extracted records are filtered, returns (lastExtractedCursor, nil).
// Returns (nil, nil) when no more records exist.
func (p *Pipeline[S, T, C]) processEpoch(ctx, drainCtx context.Context, cursor *C, stats *Stats) (*C, error) {
	// Collect records for this epoch
	checkpointSize := p.checkpoint.CheckpointInterval()
	records := make([]S, 0, checkpointSize)
	var lastCursor *C          // Cursor of last record that passed filter (for checkpoint)
	var lastExtractedCursor *C // Cursor of last extracted record (for advancement)
	count := 0
	shutdownRequested := false

extractLoop:
	for record, err := range p.job.Extract(ctx, cursor) {
		// Check for shutdown signal
		select {
		case <-ctx.Done():
			// Shutdown requested - stop extracting, process what we have
			shutdownRequested = true
			break extractLoop
		default:
		}

		if err != nil {
			stats.incErrors(1)
			if p.errHandler != nil {
				action := p.errHandler.OnError(ctx, StageExtract, err)
				if action == ActionSkip {
					continue
				}
			}
			return nil, fmt.Errorf("extract: %w", err)
		}

		stats.incExtracted(1)
		cur := p.checkpoint.Cursor(record)
		lastExtractedCursor = &cur

		// Apply filter if configured
		if p.filter != nil && !p.filter.Include(record) {
			stats.incFiltered(1)
			continue
		}

		records = append(records, record)
		lastCursor = lastExtractedCursor
		count++

		if count >= checkpointSize {
			break
		}
	}

	// No records extracted and no shutdown - we're done with the source
	if lastExtractedCursor == nil && !shutdownRequested {
		return nil, nil //nolint:nilnil // nil cursor signals completion, not an error
	}

	// If we have records to process, do so (even during shutdown)
	if len(records) > 0 {
		// Use drainCtx for transform/load so they can complete during graceful shutdown
		if err := p.processRecords(drainCtx, records, stats); err != nil {
			return nil, err
		}
		return lastCursor, nil
	}

	// All records filtered - return cursor to advance but don't process
	if lastExtractedCursor != nil {
		return lastExtractedCursor, nil
	}

	// Shutdown requested with no records collected
	return nil, nil //nolint:nilnil // nil cursor signals completion, not an error
}

// processRecords transforms and loads a batch of records with concurrency.
func (p *Pipeline[S, T, C]) processRecords(ctx context.Context, records []S, stats *Stats) error {
	// Transform all records (with concurrency)
	allResults, err := p.transformRecords(ctx, records, stats)
	if err != nil {
		return err
	}

	if len(allResults) == 0 {
		return nil
	}

	// Batch the results
	batches := p.resolveBatcher().Batch(allResults)

	// Load all batches (with concurrency)
	return p.loadBatches(ctx, batches, stats)
}

// transformRecords transforms records concurrently.
func (p *Pipeline[S, T, C]) transformRecords(ctx context.Context, records []S, stats *Stats) ([]T, error) {
	group, ctx := errgroup.WithContext(ctx)

	// Channel for records to transform
	recordCh := make(chan S, len(records))
	for _, r := range records {
		recordCh <- r
	}
	close(recordCh)

	// Channel for transformed results
	resultCh := make(chan []T, len(records))

	// Start transform workers
	numWorkers := p.resolveTransformWorkers()
	for range numWorkers {
		group.Go(func() error {
			for record := range recordCh {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				results, err := p.transform(ctx, record)
				if err != nil {
					stats.incErrors(1)
					if p.errHandler != nil {
						action := p.errHandler.OnError(ctx, StageTransform, err)
						if action == ActionSkip {
							continue
						}
					}
					return fmt.Errorf("transform: %w", err)
				}

				stats.incTransformed(1)

				if len(results) > 0 {
					select {
					case resultCh <- results:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			}
			return nil
		})
	}

	// Wait for all workers and close result channel
	err := group.Wait()
	close(resultCh)

	if err != nil {
		return nil, err
	}

	// Collect results
	var allResults []T
	for results := range resultCh {
		allResults = append(allResults, results...)
	}

	return allResults, nil
}

// loadBatches loads batches concurrently.
func (p *Pipeline[S, T, C]) loadBatches(ctx context.Context, batches [][]T, stats *Stats) error {
	group, ctx := errgroup.WithContext(ctx)

	// Channel for batches to load
	batchCh := make(chan []T, len(batches))
	for _, b := range batches {
		batchCh <- b
	}
	close(batchCh)

	reportEvery := int64(p.resolveReportInterval())

	// Start load workers
	numWorkers := p.resolveLoadWorkers()
	for range numWorkers {
		group.Go(func() error {
			for batch := range batchCh {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				if err := p.job.Load(ctx, batch); err != nil {
					stats.incErrors(1)
					if p.errHandler != nil {
						action := p.errHandler.OnError(ctx, StageLoad, err)
						if action == ActionSkip {
							continue
						}
					}
					return fmt.Errorf("load: %w", err)
				}

				// Use the returned value from incLoaded for race-free progress tracking.
				// The atomic Add returns the new total, so we can compute both the
				// previous and current values without a separate Load call.
				newLoaded := stats.incLoaded(int64(len(batch)))
				prevLoaded := newLoaded - int64(len(batch))

				// Report progress when crossing a reportEvery threshold
				if p.progress != nil && newLoaded/reportEvery > prevLoaded/reportEvery {
					p.progress.OnProgress(ctx, stats)
				}
			}
			return nil
		})
	}

	return group.Wait()
}
