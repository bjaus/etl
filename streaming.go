package etl

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"
)

// runStreaming executes the pipeline in streaming mode with no checkpoint overhead.
// Returns (drainedSuccessfully, error) where drainedSuccessfully is true if:
// - The parent context was cancelled AND
// - All in-flight operations completed within the drain timeout
func (p *Pipeline[S, T, C]) runStreaming(ctx, drainCtx context.Context, cursor *C, stats *Stats) (bool, error) {
	// Use drainCtx for the errgroup so transform/batch/load can continue draining
	// even after the parent context is cancelled
	group, groupCtx := errgroup.WithContext(drainCtx)

	// Set up channels
	transformWorkerCount := p.resolveTransformWorkers()
	loadWorkerCount := p.resolveLoadWorkers()
	extractCh := make(chan S, transformWorkerCount)
	transformCh := make(chan []T, transformWorkerCount)
	batchCh := make(chan []T, loadWorkerCount)

	// Start extraction - uses ctx (parent) so it stops when shutdown requested
	group.Go(func() error {
		return p.runExtract(ctx, groupCtx, cursor, extractCh, stats)
	})

	// Start transformation - uses groupCtx (drain context) to continue draining
	group.Go(func() error {
		return p.runTransform(groupCtx, extractCh, transformCh, stats)
	})

	// Start batching - uses groupCtx (drain context) to continue draining
	group.Go(func() error {
		return p.runBatch(groupCtx, transformCh, batchCh)
	})

	// Start loading - uses groupCtx (drain context) to continue draining
	group.Go(func() error {
		return p.runLoad(groupCtx, batchCh, stats)
	})

	err := group.Wait()

	// Determine if we drained successfully:
	// - Parent context was cancelled (shutdown requested)
	// - No error from workers (drain completed)
	drainedSuccessfully := ctx.Err() != nil && err == nil

	return drainedSuccessfully, err
}

// runExtract extracts records and sends them to the transform stage.
// ctx is checked for shutdown signal (stop extracting), drainCtx is used for channel sends.
// When ctx is cancelled, returns nil (not error) to allow graceful drain of downstream stages.
func (p *Pipeline[S, T, C]) runExtract(ctx, drainCtx context.Context, cursor *C, out chan<- S, stats *Stats) error {
	defer close(out)

	for record, err := range p.job.Extract(ctx, cursor) {
		// Check for shutdown signal - stop extracting but allow drain
		select {
		case <-ctx.Done():
			return nil // Return nil to allow graceful drain
		default:
		}

		// Handle extraction errors
		if err != nil {
			stats.incErrors(1)
			if p.errHandler != nil {
				action := p.errHandler.OnError(ctx, StageExtract, err)
				if action == ActionSkip {
					continue
				}
			}
			return fmt.Errorf("extract: %w", err)
		}

		stats.incExtracted(1)

		// Apply filter if configured
		if p.filter != nil && !p.filter.Include(record) {
			stats.incFiltered(1)
			continue
		}

		// Send to transform stage - use drainCtx so send can complete during drain
		select {
		case out <- record:
		case <-drainCtx.Done():
			return drainCtx.Err()
		}
	}

	return nil
}

func (p *Pipeline[S, T, C]) runTransform(ctx context.Context, in <-chan S, out chan<- []T, stats *Stats) error {
	defer close(out)

	var transformGroup errgroup.Group

	for range p.resolveTransformWorkers() {
		transformGroup.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case record, ok := <-in:
					if !ok {
						return nil
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

					// Skip if transform returned empty (filtered)
					if len(results) == 0 {
						continue
					}

					select {
					case out <- results:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			}
		})
	}

	return transformGroup.Wait()
}

func (p *Pipeline[S, T, C]) runBatch(ctx context.Context, in <-chan []T, out chan<- []T) error {
	defer close(out)

	batcher := p.resolveBatcher()
	loadBatchSize := p.resolveLoadBatchSize()

	var pending []T

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case items, ok := <-in:
			if !ok {
				// Flush remaining items
				if len(pending) > 0 {
					for _, batch := range batcher.Batch(pending) {
						if len(batch) > 0 {
							select {
							case out <- batch:
							case <-ctx.Done():
								return ctx.Err()
							}
						}
					}
				}
				return nil
			}

			pending = append(pending, items...)

			// Batch when we have enough
			if len(pending) >= loadBatchSize {
				for _, batch := range batcher.Batch(pending) {
					if len(batch) > 0 {
						select {
						case out <- batch:
						case <-ctx.Done():
							return ctx.Err()
						}
					}
				}
				pending = nil
			}
		}
	}
}

func (p *Pipeline[S, T, C]) runLoad(ctx context.Context, in <-chan []T, stats *Stats) error {
	var loadGroup errgroup.Group

	reportEvery := int64(p.resolveReportInterval())

	for range p.resolveLoadWorkers() {
		loadGroup.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case batch, ok := <-in:
					if !ok {
						return nil
					}

					err := p.job.Load(ctx, batch)
					if err != nil {
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
					newLoaded := stats.incLoaded(int64(len(batch)))
					prevLoaded := newLoaded - int64(len(batch))

					// Report progress when crossing a reportEvery threshold
					if p.progress != nil && newLoaded/reportEvery > prevLoaded/reportEvery {
						p.progress.OnProgress(ctx, stats)
					}
				}
			}
		})
	}

	return loadGroup.Wait()
}
