package etl

import (
	"context"
	"iter"
)

// Stage identifies where in the pipeline an event occurred.
type Stage string

const (
	StageExtract   Stage = "extract"
	StageTransform Stage = "transform"
	StageLoad      Stage = "load"
)

// Action tells the pipeline what to do after an error.
type Action string

const (
	ActionFail Action = "fail" // Stop pipeline and return error
	ActionSkip Action = "skip" // Skip this record and continue
)

// Job defines the core ETL operations. This is the only required interface to
// implement.
//
// The type parameters are:
//   - S: source record type (extracted from the data source)
//   - T: target record type (loaded to the destination)
//   - C: cursor type for checkpoint-based resumability (use any if not needed)
//
// For transformation, implement one of:
//   - [Transformer]: 1:1 transform (one input record -> one output record)
//   - [Expander]: 1:N transform (one input record -> multiple output records)
//
// If both are implemented, Transformer takes precedence.
type Job[S, T any, C comparable] interface {
	// Extract yields records from the source.
	// cursor is nil on fresh start, or the last checkpoint cursor on resume.
	// If Checkpointer[S, C] is not implemented, cursor is always nil.
	Extract(ctx context.Context, cursor *C) iter.Seq2[S, error]

	// Load writes a batch of records to the destination.
	// Should be idempotent (UPSERT) to handle potential re-processing.
	Load(ctx context.Context, batch []T) error
}

// Transformer converts one input record to one output record. Use this for
// simple 1:1 mappings where each source record produces exactly one target
// record.
//
// When to use Transformer vs Expander:
//   - Transformer: field mapping, format conversion, enrichment — one in, one out
//   - Expander: denormalization, splitting — one source record produces a variable
//     number of output records (including zero, which acts as a filter)
//
// Example:
//
//	func (j *MyJob) Transform(ctx context.Context, src Source) (Target, error) {
//	    return Target{
//	        ID:   src.ID,
//	        Name: strings.ToUpper(src.Name),
//	    }, nil
//	}
type Transformer[S, T any] interface {
	Transform(ctx context.Context, src S) (T, error)
}

// Expander converts one input record to multiple output records. Use this when
// a single source record needs to produce a variable number of target records.
//
// Returning an empty or nil slice effectively filters the record out — no
// target records are produced and nothing reaches the load stage.
//
// When to use:
//   - Denormalization: a source row with nested items produces one target per item
//   - Splitting: a source record contains multiple logical entities
//   - Conditional expansion: some records produce targets, others don't
//
// Example:
//
//	func (j *MyJob) Expand(ctx context.Context, src Source) ([]Target, error) {
//	    targets := make([]Target, 0, len(src.Items))
//	    for _, item := range src.Items {
//	        targets = append(targets, Target{ParentID: src.ID, ItemID: item.ID})
//	    }
//	    return targets, nil
//	}
type Expander[S, T any] interface {
	Expand(ctx context.Context, src S) ([]T, error)
}
