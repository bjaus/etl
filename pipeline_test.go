package etl_test

import (
	"context"
	"errors"
	"iter"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bjaus/etl"
)

// =============================================================================
// Test Helpers
// =============================================================================

// testRecord is a simple source record for testing.
type testRecord struct {
	ID    int
	Value string
}

// testOutput is a simple target record for testing.
type testOutput struct {
	ID      int
	Doubled string
}

// extractFrom is a helper that creates an Extract function from a slice of records,
// resuming from the given cursor position.
func extractFrom(records []testRecord) func(_ context.Context, cursor *int) iter.Seq2[testRecord, error] {
	return func(_ context.Context, cursor *int) iter.Seq2[testRecord, error] {
		return func(yield func(testRecord, error) bool) {
			startIdx := 0
			if cursor != nil {
				for i, r := range records {
					if r.ID == *cursor {
						startIdx = i + 1
						break
					}
				}
			}
			for _, r := range records[startIdx:] {
				if !yield(r, nil) {
					return
				}
			}
		}
	}
}

// =============================================================================
// Minimal Job Implementation
// =============================================================================

// minimalJob implements only the required Job interface with Transformer.
type minimalJob struct {
	records []testRecord
	loaded  [][]testOutput
}

var (
	_ etl.Job[testRecord, testOutput, int]    = (*minimalJob)(nil)
	_ etl.Transformer[testRecord, testOutput] = (*minimalJob)(nil)
)

func (j *minimalJob) Extract(_ context.Context, _ *int) iter.Seq2[testRecord, error] {
	return func(yield func(testRecord, error) bool) {
		for _, r := range j.records {
			if !yield(r, nil) {
				return
			}
		}
	}
}

func (j *minimalJob) Transform(_ context.Context, src testRecord) (testOutput, error) {
	return testOutput{ID: src.ID, Doubled: src.Value + src.Value}, nil
}

func (j *minimalJob) Load(_ context.Context, batch []testOutput) error {
	j.loaded = append(j.loaded, batch)
	return nil
}

// =============================================================================
// Full-Featured Job Implementation
// =============================================================================

// fullJob implements most optional interfaces (not Checkpointer - that's tested separately).
type fullJob struct {
	records         []testRecord
	loaded          [][]testOutput
	started         bool
	stopped         bool
	errorsCaught    int
	filterPredicate func(testRecord) bool
}

var (
	_ etl.Job[testRecord, testOutput, int]    = (*fullJob)(nil)
	_ etl.Transformer[testRecord, testOutput] = (*fullJob)(nil)
	_ etl.Filter[testRecord]                  = (*fullJob)(nil)
	_ etl.ErrorHandler                        = (*fullJob)(nil)
	_ etl.LoadBatchSize                       = (*fullJob)(nil)
	_ etl.TransformWorkers                    = (*fullJob)(nil)
	_ etl.LoadWorkers                         = (*fullJob)(nil)
	_ etl.Starter                             = (*fullJob)(nil)
	_ etl.Stopper                             = (*fullJob)(nil)
)

func (j *fullJob) Extract(_ context.Context, _ *int) iter.Seq2[testRecord, error] {
	return func(yield func(testRecord, error) bool) {
		for _, r := range j.records {
			if !yield(r, nil) {
				return
			}
		}
	}
}

func (j *fullJob) Transform(_ context.Context, src testRecord) (testOutput, error) {
	return testOutput{ID: src.ID, Doubled: src.Value + src.Value}, nil
}

func (j *fullJob) Load(_ context.Context, batch []testOutput) error {
	j.loaded = append(j.loaded, batch)
	return nil
}

func (j *fullJob) Include(src testRecord) bool {
	if j.filterPredicate != nil {
		return j.filterPredicate(src)
	}
	return true
}

func (j *fullJob) OnError(_ context.Context, _ etl.Stage, _ error) etl.Action {
	j.errorsCaught++
	return etl.ActionSkip
}

func (j *fullJob) LoadBatchSize() int { return 50 }

func (j *fullJob) TransformWorkers() int { return 2 }

func (j *fullJob) LoadWorkers() int { return 2 }

func (j *fullJob) Start(ctx context.Context) context.Context {
	j.started = true
	return ctx
}

func (j *fullJob) Stop(_ context.Context, _ *etl.Stats, _ error) {
	j.stopped = true
}

// =============================================================================
// Pipeline Tests
// =============================================================================

func TestPipeline_MinimalJob(t *testing.T) {
	job := &minimalJob{
		records: []testRecord{
			{ID: 1, Value: "a"},
			{ID: 2, Value: "b"},
			{ID: 3, Value: "c"},
		},
	}

	err := etl.New[testRecord, testOutput, int](job).Run(context.Background())
	require.NoError(t, err)

	total := 0
	for _, batch := range job.loaded {
		total += len(batch)
	}
	require.Equal(t, 3, total)
}

func TestPipeline_EmptyJob(t *testing.T) {
	job := &minimalJob{records: []testRecord{}}

	err := etl.New[testRecord, testOutput, int](job).Run(context.Background())
	require.NoError(t, err)
	require.Empty(t, job.loaded)
}

func TestPipeline_WithFilter(t *testing.T) {
	job := &fullJob{
		records: []testRecord{
			{ID: 1, Value: "a"},
			{ID: 2, Value: "b"},
			{ID: 3, Value: "c"},
		},
		filterPredicate: func(r testRecord) bool {
			return r.ID%2 == 1 // Only odd IDs
		},
	}

	err := etl.New[testRecord, testOutput, int](job).Run(context.Background())
	require.NoError(t, err)

	total := 0
	for _, batch := range job.loaded {
		total += len(batch)
	}
	require.Equal(t, 2, total)
}

func TestPipeline_StarterStopper(t *testing.T) {
	job := &fullJob{
		records: []testRecord{{ID: 1, Value: "a"}},
	}

	err := etl.New[testRecord, testOutput, int](job).Run(context.Background())
	require.NoError(t, err)
	require.True(t, job.started, "Starter should be called")
	require.True(t, job.stopped, "Stopper should be called")
}

// =============================================================================
// Checkpointing Job Implementation
// =============================================================================

// checkpointingJob implements Checkpointer for testing epoch-based execution.
type checkpointingJob struct {
	records           []testRecord
	loaded            [][]testOutput
	checkpoints       []int
	clearedCheckpoint bool
}

var (
	_ etl.Job[testRecord, testOutput, int]    = (*checkpointingJob)(nil)
	_ etl.Transformer[testRecord, testOutput] = (*checkpointingJob)(nil)
	_ etl.Checkpointer[testRecord, int]       = (*checkpointingJob)(nil)
)

func (j *checkpointingJob) Extract(ctx context.Context, cursor *int) iter.Seq2[testRecord, error] {
	return extractFrom(j.records)(ctx, cursor)
}

func (j *checkpointingJob) Transform(_ context.Context, src testRecord) (testOutput, error) {
	return testOutput{ID: src.ID, Doubled: src.Value + src.Value}, nil
}

func (j *checkpointingJob) Load(_ context.Context, batch []testOutput) error {
	j.loaded = append(j.loaded, batch)
	return nil
}

func (j *checkpointingJob) CheckpointInterval() int { return 2 }

func (j *checkpointingJob) Cursor(src testRecord) int { return src.ID }

func (j *checkpointingJob) LoadCheckpoint(_ context.Context) (*int, *etl.Stats, error) {
	return nil, nil, nil
}

func (j *checkpointingJob) SaveCheckpoint(_ context.Context, cursor int, _ *etl.Stats) error {
	j.checkpoints = append(j.checkpoints, cursor)
	return nil
}

func (j *checkpointingJob) ClearCheckpoint(_ context.Context) error {
	j.clearedCheckpoint = true
	return nil
}

func TestPipeline_Checkpointing(t *testing.T) {
	job := &checkpointingJob{
		records: []testRecord{
			{ID: 0, Value: "a"},
		},
	}

	err := etl.New[testRecord, testOutput, int](job).Run(context.Background())
	require.NoError(t, err)
	require.True(t, job.clearedCheckpoint, "Should clear checkpoint on success")

	total := 0
	for _, batch := range job.loaded {
		total += len(batch)
	}
	require.Equal(t, 1, total)
}

func TestPipeline_Checkpointing_MultipleEpochs(t *testing.T) {
	job := &checkpointingJob{
		records: []testRecord{
			{ID: 0, Value: "a"},
			{ID: 1, Value: "b"},
			{ID: 2, Value: "c"},
			{ID: 3, Value: "d"},
			{ID: 4, Value: "e"},
		},
	}

	err := etl.New[testRecord, testOutput, int](job).Run(context.Background())
	require.NoError(t, err)

	require.NotEmpty(t, job.checkpoints, "Should have saved checkpoints")
	require.True(t, job.clearedCheckpoint, "Should clear checkpoint on success")
}

// resumingJob simulates a job resuming from a checkpoint with saved stats.
type resumingJob struct {
	records           []testRecord
	loaded            [][]testOutput
	savedCursor       *int
	savedStats        *etl.Stats
	clearedCheckpoint bool
	finalStats        *etl.Stats
}

var (
	_ etl.Job[testRecord, testOutput, int]    = (*resumingJob)(nil)
	_ etl.Transformer[testRecord, testOutput] = (*resumingJob)(nil)
	_ etl.Checkpointer[testRecord, int]       = (*resumingJob)(nil)
)

func (j *resumingJob) Extract(ctx context.Context, cursor *int) iter.Seq2[testRecord, error] {
	return extractFrom(j.records)(ctx, cursor)
}

func (j *resumingJob) Transform(_ context.Context, src testRecord) (testOutput, error) {
	return testOutput{ID: src.ID, Doubled: src.Value + src.Value}, nil
}

func (j *resumingJob) Load(_ context.Context, batch []testOutput) error {
	j.loaded = append(j.loaded, batch)
	return nil
}

func (j *resumingJob) CheckpointInterval() int { return 2 }

func (j *resumingJob) Cursor(src testRecord) int { return src.ID }

func (j *resumingJob) LoadCheckpoint(_ context.Context) (*int, *etl.Stats, error) {
	return j.savedCursor, j.savedStats, nil
}

func (j *resumingJob) SaveCheckpoint(_ context.Context, cursor int, stats *etl.Stats) error {
	j.savedCursor = &cursor
	j.finalStats = stats
	return nil
}

func (j *resumingJob) ClearCheckpoint(_ context.Context) error {
	j.clearedCheckpoint = true
	return nil
}

func TestPipeline_Checkpointing_StatsRestoration(t *testing.T) {
	savedStats := &etl.Stats{}
	err := savedStats.UnmarshalJSON([]byte(`{"extracted":100,"transformed":95,"loaded":90,"errors":5}`))
	require.NoError(t, err)

	cursor := 1
	job := &resumingJob{
		records: []testRecord{
			{ID: 0, Value: "a"},
			{ID: 1, Value: "b"},
			{ID: 2, Value: "c"},
			{ID: 3, Value: "d"},
		},
		savedCursor: &cursor,
		savedStats:  savedStats,
	}

	err = etl.New[testRecord, testOutput, int](job).Run(context.Background())
	require.NoError(t, err)

	require.NotNil(t, job.finalStats)
	require.GreaterOrEqual(t, job.finalStats.Extracted(), int64(102))
	require.GreaterOrEqual(t, job.finalStats.Loaded(), int64(92))

	require.True(t, job.clearedCheckpoint, "Should clear checkpoint on success")
}

func TestPipeline_ConfigOverrides(t *testing.T) {
	job := &minimalJob{
		records: []testRecord{{ID: 1, Value: "a"}},
	}

	err := etl.New[testRecord, testOutput, int](job).
		WithTransformWorkers(4).
		WithLoadWorkers(2).
		WithLoadBatchSize(100).
		WithReportInterval(1000).
		Run(context.Background())

	require.NoError(t, err)
}

func TestPipeline_ContextCancellation_GracefulShutdown(t *testing.T) {
	job := &minimalJob{
		records: make([]testRecord, 1000),
	}
	for i := range job.records {
		job.records[i] = testRecord{ID: i, Value: "x"}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := etl.New[testRecord, testOutput, int](job).Run(ctx)
	require.NoError(t, err)
}

func TestPipeline_ContextCancellation_Disabled(t *testing.T) {
	job := &minimalJob{
		records: make([]testRecord, 1000),
	}
	for i := range job.records {
		job.records[i] = testRecord{ID: i, Value: "x"}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := etl.New[testRecord, testOutput, int](job).
		WithDrainTimeout(0).
		Run(ctx)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))
}

// =============================================================================
// Error Handling Tests
// =============================================================================

// errorJob is a job that produces errors in different stages.
type errorJob struct {
	records      []testRecord
	transformErr error
	loadErr      error
	extractErr   error
	loaded       [][]testOutput
	skipOnError  bool
	errorsCaught int
}

var (
	_ etl.Job[testRecord, testOutput, int]    = (*errorJob)(nil)
	_ etl.Transformer[testRecord, testOutput] = (*errorJob)(nil)
	_ etl.ErrorHandler                        = (*errorJob)(nil)
)

func (j *errorJob) Extract(_ context.Context, _ *int) iter.Seq2[testRecord, error] {
	return func(yield func(testRecord, error) bool) {
		for i, r := range j.records {
			if j.extractErr != nil && i == 1 {
				if !yield(testRecord{}, j.extractErr) {
					return
				}
				continue
			}
			if !yield(r, nil) {
				return
			}
		}
	}
}

func (j *errorJob) Transform(_ context.Context, src testRecord) (testOutput, error) {
	if j.transformErr != nil && src.ID == 1 {
		return testOutput{}, j.transformErr
	}
	return testOutput{ID: src.ID, Doubled: src.Value + src.Value}, nil
}

func (j *errorJob) Load(_ context.Context, batch []testOutput) error {
	if j.loadErr != nil {
		return j.loadErr
	}
	j.loaded = append(j.loaded, batch)
	return nil
}

func (j *errorJob) OnError(_ context.Context, _ etl.Stage, _ error) etl.Action {
	j.errorsCaught++
	if j.skipOnError {
		return etl.ActionSkip
	}
	return etl.ActionFail
}

func TestPipeline_ExtractError_Fail(t *testing.T) {
	extractErr := errors.New("extract failed")
	job := &errorJob{
		records:     []testRecord{{ID: 0, Value: "a"}, {ID: 1, Value: "b"}},
		extractErr:  extractErr,
		skipOnError: false,
	}

	err := etl.New[testRecord, testOutput, int](job).Run(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, "extract")
}

func TestPipeline_ExtractError_Skip(t *testing.T) {
	extractErr := errors.New("extract failed")
	job := &errorJob{
		records:     []testRecord{{ID: 0, Value: "a"}, {ID: 1, Value: "b"}, {ID: 2, Value: "c"}},
		extractErr:  extractErr,
		skipOnError: true,
	}

	err := etl.New[testRecord, testOutput, int](job).Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, job.errorsCaught)
}

func TestPipeline_TransformError_Fail(t *testing.T) {
	transformErr := errors.New("transform failed")
	job := &errorJob{
		records:      []testRecord{{ID: 0, Value: "a"}, {ID: 1, Value: "b"}},
		transformErr: transformErr,
		skipOnError:  false,
	}

	err := etl.New[testRecord, testOutput, int](job).Run(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, "transform")
}

func TestPipeline_TransformError_Skip(t *testing.T) {
	transformErr := errors.New("transform failed")
	job := &errorJob{
		records:      []testRecord{{ID: 0, Value: "a"}, {ID: 1, Value: "b"}, {ID: 2, Value: "c"}},
		transformErr: transformErr,
		skipOnError:  true,
	}

	err := etl.New[testRecord, testOutput, int](job).Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, job.errorsCaught)
}

func TestPipeline_LoadError_Fail(t *testing.T) {
	loadErr := errors.New("load failed")
	job := &errorJob{
		records:     []testRecord{{ID: 0, Value: "a"}},
		loadErr:     loadErr,
		skipOnError: false,
	}

	err := etl.New[testRecord, testOutput, int](job).Run(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, "load")
}

func TestPipeline_LoadError_Skip(t *testing.T) {
	loadErr := errors.New("load failed")
	job := &errorJob{
		records:     []testRecord{{ID: 0, Value: "a"}, {ID: 1, Value: "b"}},
		loadErr:     loadErr,
		skipOnError: true,
	}

	err := etl.New[testRecord, testOutput, int](job).Run(context.Background())
	require.NoError(t, err)
	require.GreaterOrEqual(t, job.errorsCaught, 1)
}

// =============================================================================
// Stats Tests
// =============================================================================

func TestStats(t *testing.T) {
	job := &minimalJob{
		records: []testRecord{
			{ID: 1, Value: "a"},
			{ID: 2, Value: "b"},
			{ID: 3, Value: "c"},
		},
	}

	err := etl.New[testRecord, testOutput, int](job).Run(context.Background())
	require.NoError(t, err)

	stats := etl.Stats{}
	stats.Extracted()
	stats.Filtered()
	stats.Transformed()
	stats.Loaded()
	stats.Errors()
	stats.LogValue()
}

// =============================================================================
// Progress Reporter Tests
// =============================================================================

// progressJob implements ProgressReporter.
type progressJob struct {
	minimalJob
	progressCalls int
}

func (j *progressJob) ReportInterval() int { return 1 }

func (j *progressJob) OnProgress(_ context.Context, _ *etl.Stats) {
	j.progressCalls++
}

var _ etl.ProgressReporter = (*progressJob)(nil)

func TestPipeline_ProgressReporter(t *testing.T) {
	job := &progressJob{
		minimalJob: minimalJob{
			records: []testRecord{
				{ID: 1, Value: "a"},
				{ID: 2, Value: "b"},
				{ID: 3, Value: "c"},
			},
		},
	}

	err := etl.New[testRecord, testOutput, int](job).Run(context.Background())
	require.NoError(t, err)
	require.Greater(t, job.progressCalls, 0, "Progress should have been reported at least once")
}

// =============================================================================
// Checkpoint Error Tests
// =============================================================================

// checkpointErrorJob tests checkpoint error handling.
type checkpointErrorJob struct {
	checkpointingJob
	loadCheckpointErr  error
	saveCheckpointErr  error
	clearCheckpointErr error
}

func (j *checkpointErrorJob) LoadCheckpoint(_ context.Context) (*int, *etl.Stats, error) {
	return nil, nil, j.loadCheckpointErr
}

func (j *checkpointErrorJob) SaveCheckpoint(_ context.Context, _ int, _ *etl.Stats) error {
	return j.saveCheckpointErr
}

func (j *checkpointErrorJob) ClearCheckpoint(_ context.Context) error {
	return j.clearCheckpointErr
}

func TestPipeline_LoadCheckpointError(t *testing.T) {
	job := &checkpointErrorJob{
		checkpointingJob: checkpointingJob{
			records: []testRecord{{ID: 0, Value: "a"}},
		},
		loadCheckpointErr: errors.New("load checkpoint failed"),
	}

	err := etl.New[testRecord, testOutput, int](job).Run(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, "load checkpoint")
}

func TestPipeline_SaveCheckpointError(t *testing.T) {
	job := &checkpointErrorJob{
		checkpointingJob: checkpointingJob{
			records: []testRecord{{ID: 0, Value: "a"}, {ID: 1, Value: "b"}, {ID: 2, Value: "c"}},
		},
		saveCheckpointErr: errors.New("save checkpoint failed"),
	}

	err := etl.New[testRecord, testOutput, int](job).Run(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, "save checkpoint")
}

func TestPipeline_ClearCheckpointError(t *testing.T) {
	job := &checkpointErrorJob{
		checkpointingJob: checkpointingJob{
			records: []testRecord{{ID: 0, Value: "a"}},
		},
		clearCheckpointErr: errors.New("clear checkpoint failed"),
	}

	err := etl.New[testRecord, testOutput, int](job).Run(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, "clear checkpoint")
}

// =============================================================================
// Missing Interface Tests
// =============================================================================

// jobWithoutTransform only implements Job, not Transformer or Expander.
type jobWithoutTransform struct {
	records []testRecord
	loaded  [][]testOutput
}

var _ etl.Job[testRecord, testOutput, int] = (*jobWithoutTransform)(nil)

func (j *jobWithoutTransform) Extract(_ context.Context, _ *int) iter.Seq2[testRecord, error] {
	return func(yield func(testRecord, error) bool) {
		for _, r := range j.records {
			if !yield(r, nil) {
				return
			}
		}
	}
}

func (j *jobWithoutTransform) Load(_ context.Context, batch []testOutput) error {
	j.loaded = append(j.loaded, batch)
	return nil
}

func TestPipeline_MissingTransformerOrExpander(t *testing.T) {
	job := &jobWithoutTransform{
		records: []testRecord{{ID: 1, Value: "a"}},
	}

	require.PanicsWithValue(t,
		"etl: job must implement Transformer[S, T] or Expander[S, T]",
		func() { etl.New[testRecord, testOutput, int](job) },
	)
}

// =============================================================================
// Expander Tests
// =============================================================================

// expanderJob transforms one record into multiple outputs using the Expander interface.
type expanderJob struct {
	records []testRecord
	loaded  [][]testOutput
}

var (
	_ etl.Job[testRecord, testOutput, int] = (*expanderJob)(nil)
	_ etl.Expander[testRecord, testOutput] = (*expanderJob)(nil)
)

func (j *expanderJob) Extract(_ context.Context, _ *int) iter.Seq2[testRecord, error] {
	return func(yield func(testRecord, error) bool) {
		for _, r := range j.records {
			if !yield(r, nil) {
				return
			}
		}
	}
}

func (j *expanderJob) Expand(_ context.Context, src testRecord) ([]testOutput, error) {
	return []testOutput{
		{ID: src.ID, Doubled: src.Value + "1"},
		{ID: src.ID, Doubled: src.Value + "2"},
	}, nil
}

func (j *expanderJob) Load(_ context.Context, batch []testOutput) error {
	j.loaded = append(j.loaded, batch)
	return nil
}

func TestPipeline_Expander(t *testing.T) {
	job := &expanderJob{
		records: []testRecord{
			{ID: 1, Value: "a"},
			{ID: 2, Value: "b"},
		},
	}

	err := etl.New[testRecord, testOutput, int](job).Run(context.Background())
	require.NoError(t, err)

	total := 0
	for _, batch := range job.loaded {
		total += len(batch)
	}
	require.Equal(t, 4, total)
}

// expanderFilterJob uses Expander to filter by returning empty slice.
type expanderFilterJob struct {
	records []testRecord
	loaded  [][]testOutput
}

var (
	_ etl.Job[testRecord, testOutput, int] = (*expanderFilterJob)(nil)
	_ etl.Expander[testRecord, testOutput] = (*expanderFilterJob)(nil)
)

func (j *expanderFilterJob) Extract(_ context.Context, _ *int) iter.Seq2[testRecord, error] {
	return func(yield func(testRecord, error) bool) {
		for _, r := range j.records {
			if !yield(r, nil) {
				return
			}
		}
	}
}

func (j *expanderFilterJob) Expand(_ context.Context, src testRecord) ([]testOutput, error) {
	if src.ID%2 == 0 {
		return nil, nil
	}
	return []testOutput{{ID: src.ID, Doubled: src.Value}}, nil
}

func (j *expanderFilterJob) Load(_ context.Context, batch []testOutput) error {
	j.loaded = append(j.loaded, batch)
	return nil
}

func TestPipeline_ExpanderFiltersRecords(t *testing.T) {
	job := &expanderFilterJob{
		records: []testRecord{
			{ID: 1, Value: "a"},
			{ID: 2, Value: "b"},
			{ID: 3, Value: "c"},
			{ID: 4, Value: "d"},
		},
	}

	err := etl.New[testRecord, testOutput, int](job).Run(context.Background())
	require.NoError(t, err)

	total := 0
	for _, batch := range job.loaded {
		total += len(batch)
	}
	require.Equal(t, 2, total)
}

// =============================================================================
// Graceful Shutdown Tests
// =============================================================================

// gracefulShutdownJob implements GracefulShutdown interface for testing.
type gracefulShutdownJob struct {
	minimalJob
	drainTimeout time.Duration
}

func (j *gracefulShutdownJob) DrainTimeout() time.Duration {
	return j.drainTimeout
}

var _ etl.DrainTimeout = (*gracefulShutdownJob)(nil)

func TestPipeline_GracefulShutdown_InterfaceDetected(t *testing.T) {
	job := &gracefulShutdownJob{
		minimalJob: minimalJob{
			records: []testRecord{
				{ID: 0, Value: "a"},
				{ID: 1, Value: "b"},
			},
		},
		drainTimeout: 10 * time.Second,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := etl.New[testRecord, testOutput, int](job).Run(ctx)
	require.NoError(t, err)
}

func TestPipeline_GracefulShutdown_BuilderOverridesInterface(t *testing.T) {
	job := &gracefulShutdownJob{
		minimalJob: minimalJob{
			records: []testRecord{
				{ID: 0, Value: "a"},
				{ID: 1, Value: "b"},
			},
		},
		drainTimeout: 10 * time.Second,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := etl.New[testRecord, testOutput, int](job).
		WithDrainTimeout(0).
		Run(ctx)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))
}

func TestPipeline_GracefulShutdown_NegativeValueIgnored(t *testing.T) {
	job := &minimalJob{
		records: []testRecord{
			{ID: 0, Value: "a"},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := etl.New[testRecord, testOutput, int](job).
		WithDrainTimeout(-1 * time.Second).
		Run(ctx)
	require.NoError(t, err)
}

// =============================================================================
// Action String Tests
// =============================================================================

func TestAction_String(t *testing.T) {
	require.Equal(t, "fail", string(etl.ActionFail))
	require.Equal(t, "skip", string(etl.ActionSkip))
}

// =============================================================================
// Additional Checkpoint Coverage Tests
// =============================================================================

// filteringCheckpointJob filters all records to test the all-filtered path.
type filteringCheckpointJob struct {
	records           []testRecord
	loaded            [][]testOutput
	checkpoints       []int
	clearedCheckpoint bool
}

var (
	_ etl.Job[testRecord, testOutput, int]    = (*filteringCheckpointJob)(nil)
	_ etl.Transformer[testRecord, testOutput] = (*filteringCheckpointJob)(nil)
	_ etl.Checkpointer[testRecord, int]       = (*filteringCheckpointJob)(nil)
	_ etl.Filter[testRecord]                  = (*filteringCheckpointJob)(nil)
)

func (j *filteringCheckpointJob) Extract(_ context.Context, cursor *int) iter.Seq2[testRecord, error] {
	return extractFrom(j.records)(context.Background(), cursor)
}

func (j *filteringCheckpointJob) Transform(_ context.Context, src testRecord) (testOutput, error) {
	return testOutput{ID: src.ID, Doubled: src.Value + src.Value}, nil
}

func (j *filteringCheckpointJob) Load(_ context.Context, batch []testOutput) error {
	j.loaded = append(j.loaded, batch)
	return nil
}

func (j *filteringCheckpointJob) Include(src testRecord) bool {
	return false // Filter all records
}

func (j *filteringCheckpointJob) CheckpointInterval() int { return 2 }

func (j *filteringCheckpointJob) Cursor(src testRecord) int { return src.ID }

func (j *filteringCheckpointJob) LoadCheckpoint(_ context.Context) (*int, *etl.Stats, error) {
	return nil, nil, nil
}

func (j *filteringCheckpointJob) SaveCheckpoint(_ context.Context, cursor int, _ *etl.Stats) error {
	j.checkpoints = append(j.checkpoints, cursor)
	return nil
}

func (j *filteringCheckpointJob) ClearCheckpoint(_ context.Context) error {
	j.clearedCheckpoint = true
	return nil
}

func TestPipeline_Checkpointing_AllRecordsFiltered(t *testing.T) {
	job := &filteringCheckpointJob{
		records: []testRecord{
			{ID: 0, Value: "a"},
			{ID: 1, Value: "b"},
			{ID: 2, Value: "c"},
			{ID: 3, Value: "d"},
		},
	}

	err := etl.New[testRecord, testOutput, int](job).Run(context.Background())
	require.NoError(t, err)
	require.True(t, job.clearedCheckpoint, "Should clear checkpoint on success")
	require.Empty(t, job.loaded, "No records should be loaded when all filtered")
}

// slowCheckpointJob allows testing graceful shutdown during checkpoint processing.
type slowCheckpointJob struct {
	records           []testRecord
	loaded            [][]testOutput
	checkpoints       []int
	clearedCheckpoint bool
	extractDelay      time.Duration
	cancelAfter       int
	cancelFunc        context.CancelFunc
	extracted         int
}

var (
	_ etl.Job[testRecord, testOutput, int]    = (*slowCheckpointJob)(nil)
	_ etl.Transformer[testRecord, testOutput] = (*slowCheckpointJob)(nil)
	_ etl.Checkpointer[testRecord, int]       = (*slowCheckpointJob)(nil)
)

func (j *slowCheckpointJob) Extract(_ context.Context, cursor *int) iter.Seq2[testRecord, error] {
	return func(yield func(testRecord, error) bool) {
		start := 0
		if cursor != nil {
			start = *cursor + 1
		}
		for i := start; i < len(j.records); i++ {
			if j.extractDelay > 0 {
				time.Sleep(j.extractDelay)
			}
			j.extracted++
			if j.cancelAfter > 0 && j.extracted >= j.cancelAfter && j.cancelFunc != nil {
				j.cancelFunc()
			}
			if !yield(j.records[i], nil) {
				return
			}
		}
	}
}

func (j *slowCheckpointJob) Transform(_ context.Context, src testRecord) (testOutput, error) {
	return testOutput{ID: src.ID, Doubled: src.Value + src.Value}, nil
}

func (j *slowCheckpointJob) Load(_ context.Context, batch []testOutput) error {
	j.loaded = append(j.loaded, batch)
	return nil
}

func (j *slowCheckpointJob) CheckpointInterval() int { return 2 }

func (j *slowCheckpointJob) Cursor(src testRecord) int { return src.ID }

func (j *slowCheckpointJob) LoadCheckpoint(_ context.Context) (*int, *etl.Stats, error) {
	return nil, nil, nil
}

func (j *slowCheckpointJob) SaveCheckpoint(_ context.Context, cursor int, _ *etl.Stats) error {
	j.checkpoints = append(j.checkpoints, cursor)
	return nil
}

func (j *slowCheckpointJob) ClearCheckpoint(_ context.Context) error {
	j.clearedCheckpoint = true
	return nil
}

func TestPipeline_Checkpointing_GracefulShutdownDuringEpoch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	job := &slowCheckpointJob{
		records: []testRecord{
			{ID: 0, Value: "a"},
			{ID: 1, Value: "b"},
			{ID: 2, Value: "c"},
			{ID: 3, Value: "d"},
			{ID: 4, Value: "e"},
			{ID: 5, Value: "f"},
		},
		cancelAfter: 3, // Cancel after extracting 3 records
		cancelFunc:  cancel,
	}

	err := etl.New[testRecord, testOutput, int](job).
		WithDrainTimeout(5 * time.Second).
		Run(ctx)
	require.NoError(t, err)
	// Should have saved a checkpoint since we cancelled mid-processing
	require.NotEmpty(t, job.checkpoints, "Should have saved checkpoint during graceful shutdown")
}

// checkpointTransformErrorJob tests error handling during transform in checkpoint mode.
type checkpointTransformErrorJob struct {
	records           []testRecord
	loaded            [][]testOutput
	checkpoints       []int
	clearedCheckpoint bool
	transformCount    int
	failAfter         int
}

var (
	_ etl.Job[testRecord, testOutput, int]    = (*checkpointTransformErrorJob)(nil)
	_ etl.Transformer[testRecord, testOutput] = (*checkpointTransformErrorJob)(nil)
	_ etl.Checkpointer[testRecord, int]       = (*checkpointTransformErrorJob)(nil)
	_ etl.ErrorHandler                        = (*checkpointTransformErrorJob)(nil)
)

func (j *checkpointTransformErrorJob) Extract(_ context.Context, cursor *int) iter.Seq2[testRecord, error] {
	return extractFrom(j.records)(context.Background(), cursor)
}

func (j *checkpointTransformErrorJob) Transform(_ context.Context, src testRecord) (testOutput, error) {
	j.transformCount++
	if j.failAfter > 0 && j.transformCount > j.failAfter {
		return testOutput{}, errors.New("transform error")
	}
	return testOutput{ID: src.ID, Doubled: src.Value + src.Value}, nil
}

func (j *checkpointTransformErrorJob) Load(_ context.Context, batch []testOutput) error {
	j.loaded = append(j.loaded, batch)
	return nil
}

func (j *checkpointTransformErrorJob) OnError(_ context.Context, stage etl.Stage, _ error) etl.Action {
	if stage == etl.StageTransform {
		return etl.ActionSkip
	}
	return etl.ActionFail
}

func (j *checkpointTransformErrorJob) CheckpointInterval() int { return 3 }

func (j *checkpointTransformErrorJob) Cursor(src testRecord) int { return src.ID }

func (j *checkpointTransformErrorJob) LoadCheckpoint(_ context.Context) (*int, *etl.Stats, error) {
	return nil, nil, nil
}

func (j *checkpointTransformErrorJob) SaveCheckpoint(_ context.Context, cursor int, _ *etl.Stats) error {
	j.checkpoints = append(j.checkpoints, cursor)
	return nil
}

func (j *checkpointTransformErrorJob) ClearCheckpoint(_ context.Context) error {
	j.clearedCheckpoint = true
	return nil
}

func TestPipeline_Checkpointing_TransformErrorSkip(t *testing.T) {
	job := &checkpointTransformErrorJob{
		records: []testRecord{
			{ID: 0, Value: "a"},
			{ID: 1, Value: "b"},
			{ID: 2, Value: "c"},
			{ID: 3, Value: "d"},
		},
		failAfter: 2, // Fail on 3rd and 4th transforms
	}

	err := etl.New[testRecord, testOutput, int](job).Run(context.Background())
	require.NoError(t, err)
	require.True(t, job.clearedCheckpoint)
}

// checkpointLoadErrorJob tests error handling during load in checkpoint mode.
type checkpointLoadErrorJob struct {
	records           []testRecord
	loadCount         int
	failAfter         int
	checkpoints       []int
	clearedCheckpoint bool
}

var (
	_ etl.Job[testRecord, testOutput, int]    = (*checkpointLoadErrorJob)(nil)
	_ etl.Transformer[testRecord, testOutput] = (*checkpointLoadErrorJob)(nil)
	_ etl.Checkpointer[testRecord, int]       = (*checkpointLoadErrorJob)(nil)
	_ etl.ErrorHandler                        = (*checkpointLoadErrorJob)(nil)
)

func (j *checkpointLoadErrorJob) Extract(_ context.Context, cursor *int) iter.Seq2[testRecord, error] {
	return extractFrom(j.records)(context.Background(), cursor)
}

func (j *checkpointLoadErrorJob) Transform(_ context.Context, src testRecord) (testOutput, error) {
	return testOutput{ID: src.ID, Doubled: src.Value + src.Value}, nil
}

func (j *checkpointLoadErrorJob) Load(_ context.Context, _ []testOutput) error {
	j.loadCount++
	if j.failAfter > 0 && j.loadCount > j.failAfter {
		return errors.New("load error")
	}
	return nil
}

func (j *checkpointLoadErrorJob) OnError(_ context.Context, stage etl.Stage, _ error) etl.Action {
	if stage == etl.StageLoad {
		return etl.ActionSkip
	}
	return etl.ActionFail
}

func (j *checkpointLoadErrorJob) CheckpointInterval() int { return 2 }

func (j *checkpointLoadErrorJob) Cursor(src testRecord) int { return src.ID }

func (j *checkpointLoadErrorJob) LoadCheckpoint(_ context.Context) (*int, *etl.Stats, error) {
	return nil, nil, nil
}

func (j *checkpointLoadErrorJob) SaveCheckpoint(_ context.Context, cursor int, _ *etl.Stats) error {
	j.checkpoints = append(j.checkpoints, cursor)
	return nil
}

func (j *checkpointLoadErrorJob) ClearCheckpoint(_ context.Context) error {
	j.clearedCheckpoint = true
	return nil
}

func TestPipeline_Checkpointing_LoadErrorSkip(t *testing.T) {
	job := &checkpointLoadErrorJob{
		records: []testRecord{
			{ID: 0, Value: "a"},
			{ID: 1, Value: "b"},
			{ID: 2, Value: "c"},
			{ID: 3, Value: "d"},
		},
		failAfter: 1, // Fail on 2nd load
	}

	err := etl.New[testRecord, testOutput, int](job).Run(context.Background())
	require.NoError(t, err)
	require.True(t, job.clearedCheckpoint)
}

// checkpointProgressJob tests progress reporting in checkpoint mode.
type checkpointProgressJob struct {
	records           []testRecord
	loaded            [][]testOutput
	checkpoints       []int
	clearedCheckpoint bool
	progressCalls     int
}

var (
	_ etl.Job[testRecord, testOutput, int]    = (*checkpointProgressJob)(nil)
	_ etl.Transformer[testRecord, testOutput] = (*checkpointProgressJob)(nil)
	_ etl.Checkpointer[testRecord, int]       = (*checkpointProgressJob)(nil)
	_ etl.ProgressReporter                    = (*checkpointProgressJob)(nil)
)

func (j *checkpointProgressJob) Extract(_ context.Context, cursor *int) iter.Seq2[testRecord, error] {
	return extractFrom(j.records)(context.Background(), cursor)
}

func (j *checkpointProgressJob) Transform(_ context.Context, src testRecord) (testOutput, error) {
	return testOutput{ID: src.ID, Doubled: src.Value + src.Value}, nil
}

func (j *checkpointProgressJob) Load(_ context.Context, batch []testOutput) error {
	j.loaded = append(j.loaded, batch)
	return nil
}

func (j *checkpointProgressJob) ReportInterval() int { return 2 }

func (j *checkpointProgressJob) OnProgress(_ context.Context, _ *etl.Stats) {
	j.progressCalls++
}

func (j *checkpointProgressJob) CheckpointInterval() int { return 3 }

func (j *checkpointProgressJob) Cursor(src testRecord) int { return src.ID }

func (j *checkpointProgressJob) LoadCheckpoint(_ context.Context) (*int, *etl.Stats, error) {
	return nil, nil, nil
}

func (j *checkpointProgressJob) SaveCheckpoint(_ context.Context, cursor int, _ *etl.Stats) error {
	j.checkpoints = append(j.checkpoints, cursor)
	return nil
}

func (j *checkpointProgressJob) ClearCheckpoint(_ context.Context) error {
	j.clearedCheckpoint = true
	return nil
}

func TestPipeline_Checkpointing_ProgressReporting(t *testing.T) {
	job := &checkpointProgressJob{
		records: []testRecord{
			{ID: 0, Value: "a"},
			{ID: 1, Value: "b"},
			{ID: 2, Value: "c"},
			{ID: 3, Value: "d"},
			{ID: 4, Value: "e"},
			{ID: 5, Value: "f"},
		},
	}

	err := etl.New[testRecord, testOutput, int](job).Run(context.Background())
	require.NoError(t, err)
	require.True(t, job.clearedCheckpoint)
	require.Greater(t, job.progressCalls, 0, "Progress should have been reported")
}

// checkpointExtractErrorJob tests extract error handling in checkpoint mode.
type checkpointExtractErrorJob struct {
	records           []testRecord
	loaded            [][]testOutput
	checkpoints       []int
	clearedCheckpoint bool
	extractCount      int
	failAfter         int
}

var (
	_ etl.Job[testRecord, testOutput, int]    = (*checkpointExtractErrorJob)(nil)
	_ etl.Transformer[testRecord, testOutput] = (*checkpointExtractErrorJob)(nil)
	_ etl.Checkpointer[testRecord, int]       = (*checkpointExtractErrorJob)(nil)
	_ etl.ErrorHandler                        = (*checkpointExtractErrorJob)(nil)
)

func (j *checkpointExtractErrorJob) Extract(_ context.Context, cursor *int) iter.Seq2[testRecord, error] {
	return func(yield func(testRecord, error) bool) {
		start := 0
		if cursor != nil {
			start = *cursor + 1
		}
		for i := start; i < len(j.records); i++ {
			j.extractCount++
			if j.failAfter > 0 && j.extractCount > j.failAfter {
				if !yield(testRecord{}, errors.New("extract error")) {
					return
				}
				continue
			}
			if !yield(j.records[i], nil) {
				return
			}
		}
	}
}

func (j *checkpointExtractErrorJob) Transform(_ context.Context, src testRecord) (testOutput, error) {
	return testOutput{ID: src.ID, Doubled: src.Value + src.Value}, nil
}

func (j *checkpointExtractErrorJob) Load(_ context.Context, batch []testOutput) error {
	j.loaded = append(j.loaded, batch)
	return nil
}

func (j *checkpointExtractErrorJob) OnError(_ context.Context, stage etl.Stage, _ error) etl.Action {
	if stage == etl.StageExtract {
		return etl.ActionSkip
	}
	return etl.ActionFail
}

func (j *checkpointExtractErrorJob) CheckpointInterval() int { return 3 }

func (j *checkpointExtractErrorJob) Cursor(src testRecord) int { return src.ID }

func (j *checkpointExtractErrorJob) LoadCheckpoint(_ context.Context) (*int, *etl.Stats, error) {
	return nil, nil, nil
}

func (j *checkpointExtractErrorJob) SaveCheckpoint(_ context.Context, cursor int, _ *etl.Stats) error {
	j.checkpoints = append(j.checkpoints, cursor)
	return nil
}

func (j *checkpointExtractErrorJob) ClearCheckpoint(_ context.Context) error {
	j.clearedCheckpoint = true
	return nil
}

func TestPipeline_Checkpointing_ExtractErrorSkip(t *testing.T) {
	job := &checkpointExtractErrorJob{
		records: []testRecord{
			{ID: 0, Value: "a"},
			{ID: 1, Value: "b"},
			{ID: 2, Value: "c"},
			{ID: 3, Value: "d"},
		},
		failAfter: 2, // Fail on 3rd extract
	}

	err := etl.New[testRecord, testOutput, int](job).Run(context.Background())
	require.NoError(t, err)
	require.True(t, job.clearedCheckpoint)
}

// saveCheckpointShutdownErrorJob tests save checkpoint error during graceful shutdown.
type saveCheckpointShutdownErrorJob struct {
	records           []testRecord
	loaded            [][]testOutput
	clearedCheckpoint bool
	cancelFunc        context.CancelFunc
	extracted         int
	cancelAfter       int
}

var (
	_ etl.Job[testRecord, testOutput, int]    = (*saveCheckpointShutdownErrorJob)(nil)
	_ etl.Transformer[testRecord, testOutput] = (*saveCheckpointShutdownErrorJob)(nil)
	_ etl.Checkpointer[testRecord, int]       = (*saveCheckpointShutdownErrorJob)(nil)
)

func (j *saveCheckpointShutdownErrorJob) Extract(_ context.Context, cursor *int) iter.Seq2[testRecord, error] {
	return func(yield func(testRecord, error) bool) {
		start := 0
		if cursor != nil {
			start = *cursor + 1
		}
		for i := start; i < len(j.records); i++ {
			j.extracted++
			if j.cancelAfter > 0 && j.extracted >= j.cancelAfter && j.cancelFunc != nil {
				j.cancelFunc()
			}
			if !yield(j.records[i], nil) {
				return
			}
		}
	}
}

func (j *saveCheckpointShutdownErrorJob) Transform(_ context.Context, src testRecord) (testOutput, error) {
	return testOutput{ID: src.ID, Doubled: src.Value + src.Value}, nil
}

func (j *saveCheckpointShutdownErrorJob) Load(_ context.Context, batch []testOutput) error {
	j.loaded = append(j.loaded, batch)
	return nil
}

func (j *saveCheckpointShutdownErrorJob) CheckpointInterval() int { return 2 }

func (j *saveCheckpointShutdownErrorJob) Cursor(src testRecord) int { return src.ID }

func (j *saveCheckpointShutdownErrorJob) LoadCheckpoint(_ context.Context) (*int, *etl.Stats, error) {
	return nil, nil, nil
}

func (j *saveCheckpointShutdownErrorJob) SaveCheckpoint(_ context.Context, _ int, _ *etl.Stats) error {
	return errors.New("save checkpoint during shutdown failed")
}

func (j *saveCheckpointShutdownErrorJob) ClearCheckpoint(_ context.Context) error {
	j.clearedCheckpoint = true
	return nil
}

func TestPipeline_Checkpointing_SaveCheckpointErrorDuringShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	job := &saveCheckpointShutdownErrorJob{
		records: []testRecord{
			{ID: 0, Value: "a"},
			{ID: 1, Value: "b"},
			{ID: 2, Value: "c"},
			{ID: 3, Value: "d"},
			{ID: 4, Value: "e"},
			{ID: 5, Value: "f"},
		},
		cancelAfter: 3,
		cancelFunc:  cancel,
	}

	err := etl.New[testRecord, testOutput, int](job).
		WithDrainTimeout(5 * time.Second).
		Run(ctx)
	require.Error(t, err)
	require.ErrorContains(t, err, "save checkpoint during shutdown")
}

// cancelBetweenEpochsJob cancels context after first checkpoint save (between epochs).
type cancelBetweenEpochsJob struct {
	records           []testRecord
	loaded            [][]testOutput
	checkpoints       []int
	clearedCheckpoint bool
	cancelFunc        context.CancelFunc
}

var (
	_ etl.Job[testRecord, testOutput, int]    = (*cancelBetweenEpochsJob)(nil)
	_ etl.Transformer[testRecord, testOutput] = (*cancelBetweenEpochsJob)(nil)
	_ etl.Checkpointer[testRecord, int]       = (*cancelBetweenEpochsJob)(nil)
)

func (j *cancelBetweenEpochsJob) Extract(_ context.Context, cursor *int) iter.Seq2[testRecord, error] {
	return extractFrom(j.records)(context.Background(), cursor)
}

func (j *cancelBetweenEpochsJob) Transform(_ context.Context, src testRecord) (testOutput, error) {
	return testOutput{ID: src.ID, Doubled: src.Value + src.Value}, nil
}

func (j *cancelBetweenEpochsJob) Load(_ context.Context, batch []testOutput) error {
	j.loaded = append(j.loaded, batch)
	return nil
}

func (j *cancelBetweenEpochsJob) CheckpointInterval() int { return 2 }

func (j *cancelBetweenEpochsJob) Cursor(src testRecord) int { return src.ID }

func (j *cancelBetweenEpochsJob) LoadCheckpoint(_ context.Context) (*int, *etl.Stats, error) {
	return nil, nil, nil
}

func (j *cancelBetweenEpochsJob) SaveCheckpoint(_ context.Context, cursor int, _ *etl.Stats) error {
	j.checkpoints = append(j.checkpoints, cursor)
	// Cancel after first checkpoint save - this triggers cancellation between epochs
	if len(j.checkpoints) == 1 && j.cancelFunc != nil {
		j.cancelFunc()
	}
	return nil
}

func (j *cancelBetweenEpochsJob) ClearCheckpoint(_ context.Context) error {
	j.clearedCheckpoint = true
	return nil
}

func TestPipeline_Checkpointing_CancelBeforeNewEpoch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	job := &cancelBetweenEpochsJob{
		records: []testRecord{
			{ID: 0, Value: "a"},
			{ID: 1, Value: "b"},
			{ID: 2, Value: "c"},
			{ID: 3, Value: "d"},
			{ID: 4, Value: "e"},
			{ID: 5, Value: "f"},
		},
		cancelFunc: cancel,
	}

	err := etl.New[testRecord, testOutput, int](job).
		WithDrainTimeout(5 * time.Second).
		Run(ctx)
	require.NoError(t, err)
	// Should have saved exactly one checkpoint (first epoch) then exited cleanly
	require.Equal(t, 1, len(job.checkpoints), "Should have saved one checkpoint before cancellation")
	require.False(t, job.clearedCheckpoint, "Should not clear checkpoint on graceful shutdown")
}
