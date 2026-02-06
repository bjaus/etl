package etl_test

import (
	"context"
	"fmt"
	"iter"

	"github.com/bjaus/etl"
)

// Source is a source record type for examples.
type Source struct {
	ID   int
	Name string
}

// Target is a target record type for examples.
type Target struct {
	ID       int
	Name     string
	Category string
	Size     int
}

// =============================================================================
// Example: Basic Pipeline
// =============================================================================

type basicJob struct {
	rows []Source
}

func (j *basicJob) Extract(_ context.Context, _ *int) iter.Seq2[Source, error] {
	return func(yield func(Source, error) bool) {
		for _, r := range j.rows {
			if !yield(r, nil) {
				return
			}
		}
	}
}

func (j *basicJob) Transform(_ context.Context, src Source) (Target, error) {
	return Target{ID: src.ID, Name: src.Name + "!"}, nil
}

func (j *basicJob) Load(_ context.Context, batch []Target) error {
	for _, r := range batch {
		fmt.Printf("loaded: %d %s\n", r.ID, r.Name) //nolint:forbidigo // example output for godoc
	}
	return nil
}

func ExampleNew() {
	job := &basicJob{
		rows: []Source{
			{ID: 1, Name: "Alice"},
			{ID: 2, Name: "Bob"},
		},
	}

	err := etl.New[Source, Target, int](job).Run(context.Background())
	if err != nil {
		fmt.Println("error:", err)
	}

	// Output:
	// loaded: 1 Alice!
	// loaded: 2 Bob!
}

// =============================================================================
// Example: Pipeline with Configuration
// =============================================================================

func ExamplePipeline_Run() {
	job := &basicJob{
		rows: []Source{
			{ID: 1, Name: "Alice"},
			{ID: 2, Name: "Bob"},
			{ID: 3, Name: "Charlie"},
		},
	}

	err := etl.New[Source, Target, int](job).
		WithLoadBatchSize(2).
		WithTransformWorkers(2).
		Run(context.Background())
	if err != nil {
		fmt.Println("error:", err)
	}

	// Unordered output:
	// loaded: 1 Alice!
	// loaded: 2 Bob!
	// loaded: 3 Charlie!
}

// =============================================================================
// Example: SizeBatcher
// =============================================================================

func ExampleSizeBatcher() {
	batcher := etl.SizeBatcher[string](2)
	batches := batcher.Batch([]string{"a", "b", "c", "d", "e"})
	fmt.Println(batches)

	// Output:
	// [[a b] [c d] [e]]
}

// =============================================================================
// Example: NoBatcher
// =============================================================================

func ExampleNoBatcher() {
	batcher := etl.NoBatcher[string]()
	batches := batcher.Batch([]string{"a", "b", "c"})
	fmt.Println(batches)

	// Output:
	// [[a b c]]
}

// =============================================================================
// Example: GroupByField
// =============================================================================

func ExampleGroupByField() {
	items := []Target{
		{ID: 1, Category: "x"},
		{ID: 2, Category: "y"},
		{ID: 3, Category: "x"},
		{ID: 4, Category: "y"},
	}

	batcher := etl.GroupByField(func(t Target) string { return t.Category })
	batches := batcher.Batch(items)

	fmt.Println("batches:", len(batches))
	for _, batch := range batches {
		fmt.Printf("  category=%s count=%d\n", batch[0].Category, len(batch))
	}

	// Unordered output:
	// batches: 2
	//   category=x count=2
	//   category=y count=2
}

// =============================================================================
// Example: GroupByFieldWithSizeLimit
// =============================================================================

func ExampleGroupByFieldWithSizeLimit() {
	items := []Target{
		{ID: 1, Category: "x"},
		{ID: 2, Category: "x"},
		{ID: 3, Category: "x"},
		{ID: 4, Category: "y"},
	}

	// Group by category, but cap each group at 2 items
	batcher := etl.GroupByFieldWithSizeLimit(
		func(t Target) string { return t.Category },
		2,
	)
	batches := batcher.Batch(items)

	fmt.Println("batches:", len(batches))
	for _, batch := range batches {
		fmt.Printf("  category=%s count=%d\n", batch[0].Category, len(batch))
	}

	// Unordered output:
	// batches: 3
	//   category=x count=2
	//   category=x count=1
	//   category=y count=1
}

// =============================================================================
// Example: WeightedBatcher
// =============================================================================

func ExampleWeightedBatcher() {
	items := []Target{
		{ID: 1, Size: 30},
		{ID: 2, Size: 30},
		{ID: 3, Size: 30},
		{ID: 4, Size: 30},
		{ID: 5, Size: 30},
	}

	// Batch so total size per batch does not exceed 70
	batcher := etl.WeightedBatcher(func(t Target) int { return t.Size }, 70)
	batches := batcher.Batch(items)

	for i, batch := range batches {
		total := 0
		for _, t := range batch {
			total += t.Size
		}
		fmt.Printf("batch %d: %d items, total size %d\n", i, len(batch), total)
	}

	// Output:
	// batch 0: 2 items, total size 60
	// batch 1: 2 items, total size 60
	// batch 2: 1 items, total size 30
}

// =============================================================================
// Example: StripeBatcher
// =============================================================================

func ExampleStripeBatcher() {
	items := []Target{
		{ID: 1, Category: "a"},
		{ID: 2, Category: "b"},
		{ID: 3, Category: "a"},
		{ID: 4, Category: "c"},
		{ID: 5, Category: "b"},
		{ID: 6, Category: "a"},
	}

	// 2 stripes, max 10 items per batch
	batcher := etl.StripeBatcher(
		func(t Target) string { return t.Category },
		2,
		10,
	)
	batches := batcher.Batch(items)

	fmt.Println("batches:", len(batches))
	total := 0
	for _, batch := range batches {
		total += len(batch)
	}
	fmt.Println("total items:", total)

	// Output:
	// batches: 2
	// total items: 6
}

// =============================================================================
// Example: CombineBatchers
// =============================================================================

func ExampleCombineBatchers() {
	items := []Target{
		{ID: 1, Category: "x"},
		{ID: 2, Category: "x"},
		{ID: 3, Category: "x"},
		{ID: 4, Category: "y"},
		{ID: 5, Category: "y"},
	}

	// First group by category, then split groups into batches of at most 2
	batcher := etl.CombineBatchers(
		etl.GroupByField(func(t Target) string { return t.Category }),
		etl.SizeBatcher[Target](2),
	)
	batches := batcher.Batch(items)

	fmt.Println("batches:", len(batches))
	for _, batch := range batches {
		fmt.Printf("  category=%s count=%d\n", batch[0].Category, len(batch))
	}

	// Unordered output:
	// batches: 3
	//   category=x count=2
	//   category=x count=1
	//   category=y count=2
}
