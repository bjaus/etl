package etl

import (
	"context"
	"fmt"
	"time"
)

// transformMode indicates which transformation strategy to use.
type transformMode int

const (
	transformModeTransformer transformMode = iota // 1:1 via Transformer interface
	transformModeExpander                         // 1:N via Expander interface
)

// Pipeline orchestrates the ETL process.
type Pipeline[S, T any, C comparable] struct {
	job Job[S, T, C]

	// Configuration overrides (nil means use interface value or default)
	transformWorkerCount *int
	loadWorkerCount      *int
	batchSize            *int
	reportInterval       *int
	drainTimeout         *time.Duration

	// Transformation strategy (detected at construction)
	txMode      transformMode
	transformer Transformer[S, T]
	expander    Expander[S, T]

	// Optional capabilities (detected from job interfaces)
	filter              Filter[S]
	errHandler          ErrorHandler
	progress            ProgressReporter
	checkpoint          Checkpointer[S, C]
	starter             Starter
	stopper             Stopper
	batcher             Batcher[T]
	loadBatchSizeIface  LoadBatchSize
	reportIntervalIface ReportInterval
	transformWorkers    TransformWorkers
	loadWorkers         LoadWorkers
	drainTimeoutIface   DrainTimeout
}

// New creates a new Pipeline for the given job.
// The job must implement Job[S, T, C]. Optional interfaces are auto-detected.
//
// For transformation, the job must implement one of:
//   - Transformer[S, T]: 1:1 transform (one input record -> one output record)
//   - Expander[S, T]: 1:N transform (one input record -> multiple output records)
//
// If both Transformer and Expander are implemented, Transformer takes precedence.
// Panics if neither is implemented.
func New[S, T any, C comparable](job Job[S, T, C]) *Pipeline[S, T, C] {
	p := &Pipeline[S, T, C]{
		job: job,
	}

	// Detect transformation mode (precedence: Transformer > Expander)
	if t, ok := any(job).(Transformer[S, T]); ok {
		p.txMode = transformModeTransformer
		p.transformer = t
	} else if e, ok := any(job).(Expander[S, T]); ok {
		p.txMode = transformModeExpander
		p.expander = e
	} else {
		panic("etl: job must implement Transformer[S, T] or Expander[S, T]")
	}

	// Auto-detect optional interfaces
	if f, ok := any(job).(Filter[S]); ok {
		p.filter = f
	}
	if h, ok := any(job).(ErrorHandler); ok {
		p.errHandler = h
	}
	if t, ok := any(job).(ProgressReporter); ok {
		p.progress = t
	}
	if c, ok := any(job).(Checkpointer[S, C]); ok {
		p.checkpoint = c
	}
	if s, ok := any(job).(Starter); ok {
		p.starter = s
	}
	if s, ok := any(job).(Stopper); ok {
		p.stopper = s
	}
	if b, ok := any(job).(Batcher[T]); ok {
		p.batcher = b
	}
	if s, ok := any(job).(LoadBatchSize); ok {
		p.loadBatchSizeIface = s
	}
	if r, ok := any(job).(ReportInterval); ok {
		p.reportIntervalIface = r
	}
	if c, ok := any(job).(TransformWorkers); ok {
		p.transformWorkers = c
	}
	if c, ok := any(job).(LoadWorkers); ok {
		p.loadWorkers = c
	}
	if g, ok := any(job).(DrainTimeout); ok {
		p.drainTimeoutIface = g
	}

	return p
}

// WithTransformWorkers overrides the number of concurrent transform workers.
// Priority: this method > TransformWorkers interface > DefaultTransformWorkers.
// Values less than 1 are ignored.
func (p *Pipeline[S, T, C]) WithTransformWorkers(n int) *Pipeline[S, T, C] {
	if n >= 1 {
		p.transformWorkerCount = &n
	}
	return p
}

// WithLoadWorkers overrides the number of concurrent load workers.
// Priority: this method > LoadWorkers interface > DefaultLoadWorkers.
// Values less than 1 are ignored.
func (p *Pipeline[S, T, C]) WithLoadWorkers(n int) *Pipeline[S, T, C] {
	if n >= 1 {
		p.loadWorkerCount = &n
	}
	return p
}

// WithLoadBatchSize overrides the number of records to batch before loading.
// Priority: this method > LoadBatchSize interface > DefaultLoadBatchSize.
// Values less than 1 are ignored.
func (p *Pipeline[S, T, C]) WithLoadBatchSize(n int) *Pipeline[S, T, C] {
	if n >= 1 {
		p.batchSize = &n
	}
	return p
}

// WithReportInterval overrides how often to report progress (in records).
// Priority: this method > ProgressReporter interface > DefaultReportInterval.
// Values less than 1 are ignored.
func (p *Pipeline[S, T, C]) WithReportInterval(n int) *Pipeline[S, T, C] {
	if n >= 1 {
		p.reportInterval = &n
	}
	return p
}

// WithDrainTimeout overrides the graceful shutdown timeout.
// When the parent context is cancelled, the pipeline will wait up to this duration
// for in-flight operations to complete before forcing an abort.
// Priority: this method > DrainTimeout interface > DefaultDrainTimeout.
// Set to 0 to disable graceful shutdown (immediate abort). Negative values are ignored.
func (p *Pipeline[S, T, C]) WithDrainTimeout(d time.Duration) *Pipeline[S, T, C] {
	if d < 0 {
		return p
	}
	p.drainTimeout = &d
	return p
}

// transform applies the appropriate transformation based on the detected mode.
// Returns a slice of transformed records (may be empty, single, or multiple items).
func (p *Pipeline[S, T, C]) transform(ctx context.Context, src S) ([]T, error) {
	switch p.txMode {
	case transformModeTransformer:
		result, err := p.transformer.Transform(ctx, src)
		if err != nil {
			return nil, err
		}
		return []T{result}, nil

	case transformModeExpander:
		return p.expander.Expand(ctx, src)

	default:
		panic("etl: unknown transform mode")
	}
}

// checkpointingEnabled returns true if epoch-based checkpointing is configured.
func (p *Pipeline[S, T, C]) checkpointingEnabled() bool {
	return p.checkpoint != nil
}

// Run executes the pipeline.
func (p *Pipeline[S, T, C]) Run(ctx context.Context) error {
	stats := &Stats{}

	if p.starter != nil {
		ctx = p.starter.Start(ctx)
	}

	cursor, err := p.loadCheckpoint(ctx, stats)
	if err != nil {
		return err
	}

	drainCtx, shutdownComplete := p.setupDrainContext(ctx)
	defer close(shutdownComplete)

	drainedSuccessfully, pipelineErr := p.execute(ctx, drainCtx, cursor, stats)

	if p.stopper != nil {
		p.stopper.Stop(drainCtx, stats, pipelineErr)
	}

	return p.handleCompletion(ctx, drainedSuccessfully, pipelineErr)
}

// loadCheckpoint loads checkpoint cursor and restores stats if checkpointing is enabled.
func (p *Pipeline[S, T, C]) loadCheckpoint(ctx context.Context, stats *Stats) (*C, error) {
	if p.checkpoint == nil {
		return nil, nil //nolint:nilnil // nil cursor means start from beginning
	}

	cursor, savedStats, err := p.checkpoint.LoadCheckpoint(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load checkpoint: %w", err)
	}

	if savedStats != nil {
		stats.extracted.Store(savedStats.extracted.Load())
		stats.filtered.Store(savedStats.filtered.Load())
		stats.transformed.Store(savedStats.transformed.Load())
		stats.loaded.Store(savedStats.loaded.Load())
		stats.errors.Store(savedStats.errors.Load())
	}

	return cursor, nil
}

// setupDrainContext creates a context for graceful shutdown with two-phase management:
// - parent ctx: When cancelled, signals "stop extracting new records"
// - drainCtx: Allows in-flight transform/load operations to complete within timeout
func (p *Pipeline[S, T, C]) setupDrainContext(ctx context.Context) (context.Context, chan struct{}) {
	drainTimeout := p.resolveDrainTimeout()
	drainCtx, drainCancel := context.WithCancelCause(context.WithoutCancel(ctx))
	shutdownComplete := make(chan struct{})

	if drainTimeout > 0 {
		go p.runDrainTimer(ctx, drainTimeout, drainCancel, shutdownComplete)
	} else {
		go p.mirrorContextCancel(ctx, drainCancel, shutdownComplete)
	}

	return drainCtx, shutdownComplete
}

// runDrainTimer starts a timer when parent context is cancelled, cancelling drain context on timeout.
func (p *Pipeline[S, T, C]) runDrainTimer(ctx context.Context, timeout time.Duration, cancel context.CancelCauseFunc, done <-chan struct{}) {
	select {
	case <-ctx.Done():
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case <-timer.C:
			cancel(fmt.Errorf("drain timeout expired after %v", timeout))
		case <-done:
			cancel(nil)
		}
	case <-done:
		cancel(nil)
	}
}

// mirrorContextCancel cancels drain context when parent context is cancelled (no graceful shutdown).
func (p *Pipeline[S, T, C]) mirrorContextCancel(ctx context.Context, cancel context.CancelCauseFunc, done <-chan struct{}) {
	select {
	case <-ctx.Done():
		cancel(ctx.Err())
	case <-done:
		cancel(nil)
	}
}

// execute runs the pipeline in the appropriate mode.
func (p *Pipeline[S, T, C]) execute(ctx, drainCtx context.Context, cursor *C, stats *Stats) (bool, error) {
	if p.checkpointingEnabled() {
		return p.runWithCheckpoints(ctx, drainCtx, cursor, stats)
	}
	return p.runStreaming(ctx, drainCtx, cursor, stats)
}

// handleCompletion handles checkpoint clearing and error reporting after pipeline execution.
func (p *Pipeline[S, T, C]) handleCompletion(ctx context.Context, drainedSuccessfully bool, pipelineErr error) error {
	if pipelineErr != nil {
		return pipelineErr
	}

	if p.checkpoint != nil {
		if ctx.Err() != nil {
			// Graceful shutdown completed successfully - checkpoint was already saved
			return nil //nolint:nilerr // intentional: graceful shutdown is not an error
		}
		if err := p.checkpoint.ClearCheckpoint(ctx); err != nil {
			return fmt.Errorf("failed to clear checkpoint: %w", err)
		}
	}

	drainTimeout := p.resolveDrainTimeout()
	if ctx.Err() != nil && (drainTimeout == 0 || !drainedSuccessfully) {
		return ctx.Err()
	}

	return nil
}
