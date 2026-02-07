# etl

[![Go Reference](https://pkg.go.dev/badge/github.com/bjaus/etl.svg)](https://pkg.go.dev/github.com/bjaus/etl)
[![Go Report Card](https://goreportcard.com/badge/github.com/bjaus/etl)](https://goreportcard.com/report/github.com/bjaus/etl)
[![CI](https://github.com/bjaus/etl/actions/workflows/ci.yml/badge.svg)](https://github.com/bjaus/etl/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/bjaus/etl/branch/main/graph/badge.svg)](https://codecov.io/gh/bjaus/etl)

A flexible, high-performance Extract-Transform-Load pipeline framework for Go with interface-based configuration and checkpoint support.

## Features

- **Interface-Based Design** — Implement only the interfaces you need; the pipeline auto-detects capabilities
- **Concurrent Processing** — Configurable worker pools for transform and load stages
- **Checkpointing** — Resume interrupted pipelines from the last successful checkpoint
- **Flexible Batching** — Multiple batching strategies including size, weight, grouping, and striping
- **Graceful Shutdown** — Configurable drain timeout for clean shutdowns
- **Progress Reporting** — Built-in progress callbacks with statistics
- **Error Handling** — Per-stage error handling with skip or fail actions
- **Iterator-Based Extraction** — Uses Go 1.23+ `iter.Seq2` for memory-efficient streaming
- **Minimal Dependencies** — Only `golang.org/x/sync` (plus testify for tests)

## Installation

```bash
go get github.com/bjaus/etl
```

Requires Go 1.25 or later.

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "iter"

    "github.com/bjaus/etl"
)

type Source struct {
    ID   int
    Name string
}

type Target struct {
    ID   int
    Name string
}

type MyJob struct {
    data []Source
}

func (j *MyJob) Extract(_ context.Context, _ *int) iter.Seq2[Source, error] {
    return func(yield func(Source, error) bool) {
        for _, row := range j.data {
            if !yield(row, nil) {
                return
            }
        }
    }
}

func (j *MyJob) Transform(_ context.Context, src Source) (Target, error) {
    return Target{ID: src.ID, Name: src.Name + "!"}, nil
}

func (j *MyJob) Load(_ context.Context, batch []Target) error {
    for _, t := range batch {
        fmt.Printf("Loaded: %d %s\n", t.ID, t.Name)
    }
    return nil
}

func main() {
    job := &MyJob{
        data: []Source{
            {ID: 1, Name: "Alice"},
            {ID: 2, Name: "Bob"},
        },
    }

    err := etl.New[Source, Target, int](job).Run(context.Background())
    if err != nil {
        panic(err)
    }
}
```

## Core Interfaces

### Required: Job Interface

Your job must implement three methods:

```go
type Job[S, T any, C comparable] interface {
    Extract(ctx context.Context, cursor *C) iter.Seq2[S, error]
    Transform(ctx context.Context, src S) (T, error)
    Load(ctx context.Context, batch []T) error
}
```

### Optional Interfaces

Implement these for additional capabilities:

| Interface | Methods | Description |
|-----------|---------|-------------|
| `Filter[S]` | `Include(S) bool` | Pre-transform filtering |
| `Expander[S, T]` | `Expand(ctx, S) ([]T, error)` | One-to-many transformation |
| `Batcher[T]` | `Batch([]T) [][]T` | Custom batching logic |
| `ErrorHandler` | `OnError(ctx, Stage, error) Action` | Per-stage error handling |
| `ProgressReporter` | `ReportInterval() int`, `OnProgress(ctx, *Stats)` | Progress callbacks |
| `Checkpointer[S, C]` | `CheckpointInterval() int`, `Cursor(S) C`, `LoadCheckpoint(ctx)`, `SaveCheckpoint(ctx, C, *Stats)`, `ClearCheckpoint(ctx)` | Resumable pipelines |
| `Starter` | `Start(ctx) context.Context` | Pre-run setup |
| `Stopper` | `Stop(ctx, *Stats, error)` | Post-run cleanup |

## Configuration

Configure via method chaining or implement corresponding interfaces:

```go
err := etl.New[Source, Target, int](job).
    WithTransformWorkers(4).      // Concurrent transform workers
    WithLoadWorkers(2).           // Concurrent load workers
    WithLoadBatchSize(100).       // Records per batch
    WithReportInterval(1000).     // Progress report every N records
    WithDrainTimeout(time.Minute).// Graceful shutdown timeout
    Run(ctx)
```

Or implement interfaces in your job:

```go
func (j *MyJob) TransformWorkers() int    { return 4 }
func (j *MyJob) LoadWorkers() int         { return 2 }
func (j *MyJob) LoadBatchSize() int       { return 100 }
func (j *MyJob) DrainTimeout() time.Duration { return time.Minute }
```

Configuration priority: `WithXxx()` methods > interface implementations > defaults.

## Batching Strategies

### Size-Based (Default)

```go
batcher := etl.SizeBatcher[Target](100)
// [[0-99], [100-199], [200-249]]
```

### Weight-Based

Batch by cumulative weight (e.g., SQL parameter limits):

```go
// Each row uses 5 params; Postgres allows 65535 per statement
batcher := etl.WeightedBatcher(func(t Target) int { return 5 }, 65535)
```

### Group By Field

Keep related records together:

```go
batcher := etl.GroupByField(func(t Target) string { return t.Category })
// All items with same category in one batch
```

### Group By Field with Size Limit

```go
batcher := etl.GroupByFieldWithSizeLimit(
    func(t Target) string { return t.Category },
    50, // Max 50 items per batch
)
```

### Stripe Batcher

Hash-stripe for parallel load workers without lock contention:

```go
batcher := etl.StripeBatcher(
    func(t Target) string { return t.UserID },
    4,   // Number of stripes (match load workers)
    500, // Max batch size
)
```

### Combine Batchers

Chain multiple strategies:

```go
batcher := etl.CombineBatchers(
    etl.GroupByField(func(t Target) string { return t.Category }),
    etl.WeightedBatcher(func(t Target) int { return t.Size }, 10*1024),
)
```

### No Batching

Send everything in one batch:

```go
batcher := etl.NoBatcher[Target]()
```

## Checkpointing

For resumable pipelines, implement `Checkpointer`:

```go
func (j *MyJob) CheckpointInterval() int { return 5000 }

func (j *MyJob) Cursor(src Source) int64 { return src.ID }

func (j *MyJob) LoadCheckpoint(ctx context.Context) (*int64, *etl.Stats, error) {
    // Load from database/file
    cursor, stats, err := j.db.LoadCheckpoint(ctx, j.jobID)
    return cursor, stats, err
}

func (j *MyJob) SaveCheckpoint(ctx context.Context, cursor int64, stats *etl.Stats) error {
    return j.db.SaveCheckpoint(ctx, j.jobID, cursor, stats)
}

func (j *MyJob) ClearCheckpoint(ctx context.Context) error {
    return j.db.ClearCheckpoint(ctx, j.jobID)
}
```

When checkpointing is enabled:
- Records are processed in epochs of `CheckpointInterval()` records
- Checkpoint is saved after each successful epoch
- On restart, pipeline resumes from the last saved cursor

## Error Handling

Without `ErrorHandler`, the pipeline stops on first error. With it:

```go
func (j *MyJob) OnError(ctx context.Context, stage etl.Stage, err error) etl.Action {
    switch stage {
    case etl.StageExtract:
        slog.Warn("skipping bad record", "error", err)
        return etl.ActionSkip
    case etl.StageTransform:
        slog.Error("transform failed", "error", err)
        return etl.ActionSkip
    case etl.StageLoad:
        return etl.ActionFail // Stop on load errors
    }
    return etl.ActionFail
}
```

## Progress Reporting

```go
func (j *MyJob) ReportInterval() int { return 10000 }

func (j *MyJob) OnProgress(ctx context.Context, stats *etl.Stats) {
    slog.Info("progress",
        "extracted", stats.Extracted(),
        "transformed", stats.Transformed(),
        "loaded", stats.Loaded(),
        "errors", stats.Errors(),
    )
}
```

## Lifecycle Hooks

```go
func (j *MyJob) Start(ctx context.Context) context.Context {
    j.startTime = time.Now()
    slog.Info("pipeline starting")
    return ctx
}

func (j *MyJob) Stop(ctx context.Context, stats *etl.Stats, err error) {
    elapsed := time.Since(j.startTime)
    rate := float64(stats.Loaded()) / elapsed.Seconds()
    if err != nil {
        slog.Error("pipeline failed", "error", err, "loaded", stats.Loaded())
    } else {
        slog.Info("pipeline complete", "loaded", stats.Loaded(), "rate", rate)
    }
}
```

## Graceful Shutdown

Handle SIGINT/SIGTERM for graceful shutdown:

```go
ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
defer cancel()

err := etl.New[Source, Target, int64](job).
    WithDrainTimeout(30 * time.Second).
    Run(ctx)
```

## Execution Modes

### Streaming Mode (Default)

Records flow through the pipeline with concurrent stages. Use when checkpointing is not implemented.

### Epoch Mode

When `Checkpointer` is implemented, records are processed in epochs:
1. Extract up to `CheckpointInterval()` records
2. Transform and load with concurrency
3. Save checkpoint
4. Repeat

## Best Practices

1. **Idempotent Loads** — Use UPSERT for safe restarts and checkpoint boundaries
2. **Generous Checkpoint Intervals** — 1000-10000 balances recovery granularity vs overhead
3. **Match Load Workers to Connection Pool** — Avoid connection exhaustion
4. **Use Stripe Batching for Parallel Loads** — Prevents lock contention on same rows

## Testing

Comprehensive test coverage including concurrent scenarios:

```bash
go test -v ./...
```

## License

MIT License - see [LICENSE](LICENSE) for details.
