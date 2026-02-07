package etl

import (
	"fmt"
	"hash/fnv"
)

// Batcher groups transformed records into batches for loading. Implement this
// interface on your job when the default size-based batching is insufficient.
//
// The pipeline calls Batch after accumulating records from the transform stage.
// In streaming mode, Batch is called whenever the pending buffer reaches
// LoadBatchSize, and once more to flush remaining records. In epoch mode, Batch
// is called once per epoch with all transformed records from that epoch.
//
// The default batcher (used when Batcher is not implemented) is equivalent to
// SizeBatcher with the resolved LoadBatchSize.
//
// When to implement a custom Batcher:
//   - Records must be grouped by a key (e.g., tenant ID) so each Load call
//     targets a single partition
//   - Batch size depends on record weight (e.g., SQL parameter count limits)
//   - You need stripe-based batching for parallel load workers
//
// Ready-made batchers are available for common patterns:
//   - [SizeBatcher]: fixed number of items per batch
//   - [GroupByField]: group by a key extracted from each record
//   - [GroupByFieldWithSizeLimit]: group by key with a per-group size cap
//   - [WeightedBatcher]: batch by cumulative weight (e.g., SQL param limits)
//   - [StripeBatcher]: hash-stripe for parallel load workers
//   - [NoBatcher]: send everything in a single batch
//   - [CombineBatchers]: compose multiple strategies in sequence
//
// Example:
//
//	func (j *MyJob) Batch(items []Target) [][]Target {
//	    return etl.WeightedBatcher(func(t Target) int { return 5 }, 65535).Batch(items)
//	}
type Batcher[T any] interface {
	// Batch groups items into batches for loading.
	Batch(items []T) [][]T
}

// BatcherFunc adapts a plain function to the [Batcher] interface.
//
// Example:
//
//	batcher := etl.BatcherFunc[Target](func(items []Target) [][]Target {
//	    // custom batching logic
//	    return [][]Target{items}
//	})
type BatcherFunc[T any] func(items []T) [][]T

func (f BatcherFunc[T]) Batch(items []T) [][]T {
	return f(items)
}

// NoBatcher returns items as a single batch (no batching).
func NoBatcher[Target any]() Batcher[Target] {
	return BatcherFunc[Target](func(items []Target) [][]Target {
		if len(items) == 0 {
			return nil
		}
		return [][]Target{items}
	})
}

// SizeBatcher creates batches with a maximum number of items per batch.
//
// Example:
//
//	// Create batches of up to 100 items each
//	batcher := etl.SizeBatcher[MyRecord](100)
func SizeBatcher[Target any](maxSize int) Batcher[Target] {
	return BatcherFunc[Target](func(items []Target) [][]Target {
		if len(items) == 0 || maxSize <= 0 {
			return nil
		}
		return chunk(items, maxSize)
	})
}

// GroupByField creates batches by grouping items that have the same value for a given field.
// All items with the same key will be in the same batch, regardless of batch size.
//
// Example:
//
//	// Group all records by category
//	batcher := etl.GroupByField(func(t Target) string {
//		return t.Category
//	})
func GroupByField[Target any, K comparable](keyExtractor func(Target) K) Batcher[Target] {
	return BatcherFunc[Target](func(items []Target) [][]Target {
		if len(items) == 0 {
			return nil
		}

		groups := groupBy(items, keyExtractor)
		batches := make([][]Target, 0, len(groups))

		for _, group := range groups {
			batches = append(batches, group)
		}

		return batches
	})
}

// GroupByFieldWithSizeLimit combines field grouping with size limits per group.
// Items are first grouped by field value, then large groups are split into smaller batches.
//
// Example:
//
//	// Group by category, but limit each group to 50 records
//	batcher := etl.GroupByFieldWithSizeLimit(
//		func(t Target) string { return t.Category },
//		50,
//	)
func GroupByFieldWithSizeLimit[Target any, K comparable](
	keyExtractor func(Target) K,
	maxGroupSize int,
) Batcher[Target] {
	return BatcherFunc[Target](func(items []Target) [][]Target {
		if len(items) == 0 || maxGroupSize <= 0 {
			return nil
		}

		groups := groupBy(items, keyExtractor)
		var batches [][]Target

		for _, group := range groups {
			chunks := chunk(group, maxGroupSize)
			batches = append(batches, chunks...)
		}

		return batches
	})
}

// WeightedBatcher creates batches where the total weight does not exceed maxWeight.
// The weigher function returns the weight of each individual item. Items are accumulated
// into a batch until adding the next item would exceed maxWeight, at which point a new
// batch is started.
//
// If a single item exceeds maxWeight, it is placed in its own batch (never dropped).
//
// Common use cases:
//   - SQL parameter limits: each record contributes N parameters to an INSERT
//   - Payload size limits: each record contributes a variable number of bytes
//   - API rate/cost budgets: each record has a variable cost
//
// Example:
//
//	// Batch by SQL parameter count (5 columns per row, 65535 param limit)
//	batcher := etl.WeightedBatcher(func(t Target) int { return 5 }, 65535)
//
//	// Batch by estimated payload size
//	batcher := etl.WeightedBatcher(func(t Target) int { return len(t.Body) }, 10*1024*1024)
func WeightedBatcher[Target any](weigher func(Target) int, maxWeight int) Batcher[Target] {
	return BatcherFunc[Target](func(items []Target) [][]Target {
		if len(items) == 0 || maxWeight <= 0 {
			return nil
		}

		var batches [][]Target
		var current []Target
		currentWeight := 0

		for _, item := range items {
			w := weigher(item)

			// If adding this item would exceed the limit, flush current batch
			if len(current) > 0 && currentWeight+w > maxWeight {
				batches = append(batches, current)
				current = nil
				currentWeight = 0
			}

			current = append(current, item)
			currentWeight += w
		}

		if len(current) > 0 {
			batches = append(batches, current)
		}

		return batches
	})
}

// StripeBatcher assigns items to stripes via a hash of their key, then interleaves
// batches across stripes in round-robin order. This is designed for parallel load
// workers: items with the same key always land in the same stripe, and the
// interleaving ensures concurrent workers naturally process different stripes,
// avoiding lock contention on shared resources (e.g., database rows).
//
// Within each stripe, items are grouped by key for locality, then chunked by
// maxBatchSize. The output order is: stripe 0 batch 0, stripe 1 batch 0, ...,
// stripe 0 batch 1, stripe 1 batch 1, etc.
//
// Example:
//
//	// 4 stripes (match load worker count), max 500 items per batch
//	batcher := etl.StripeBatcher(
//	    func(r Record) string { return r.TenantID },
//	    4,   // numStripes
//	    500, // maxBatchSize
//	)
func StripeBatcher[T any, K comparable](keyFn func(T) K, numStripes int, maxBatchSize int) Batcher[T] {
	return BatcherFunc[T](func(items []T) [][]T {
		if len(items) == 0 || numStripes <= 0 || maxBatchSize <= 0 {
			return nil
		}

		// Assign each item to a stripe based on hash of its key.
		stripes := make([][]T, numStripes)
		keyToStripe := make(map[K]int)
		for _, item := range items {
			key := keyFn(item)
			stripe, ok := keyToStripe[key]
			if !ok {
				stripe = int(hashKey(key) % uint64(numStripes))
				keyToStripe[key] = stripe
			}
			stripes[stripe] = append(stripes[stripe], item)
		}

		// Within each stripe, group by key for locality then chunk.
		stripeBatches := make([][][]T, numStripes)
		for i, stripeItems := range stripes {
			if len(stripeItems) == 0 {
				continue
			}

			// Group by key within the stripe for cache/lock locality
			groups := groupBy(stripeItems, keyFn)

			var current []T
			for _, group := range groups {
				// Large group: flush current, then chunk the group directly
				if len(group) > maxBatchSize {
					if len(current) > 0 {
						stripeBatches[i] = append(stripeBatches[i], current)
						current = nil
					}
					stripeBatches[i] = append(stripeBatches[i], chunk(group, maxBatchSize)...)
					continue
				}

				// Would exceed max? Flush first
				if len(current)+len(group) > maxBatchSize {
					stripeBatches[i] = append(stripeBatches[i], current)
					current = nil
				}

				current = append(current, group...)
			}
			if len(current) > 0 {
				stripeBatches[i] = append(stripeBatches[i], current)
			}
		}

		// Interleave: round-robin across stripes so parallel workers hit different stripes.
		maxBatches := 0
		total := 0
		for _, sb := range stripeBatches {
			total += len(sb)
			if len(sb) > maxBatches {
				maxBatches = len(sb)
			}
		}

		batches := make([][]T, 0, total)
		for batchIdx := range maxBatches {
			for stripeIdx := range numStripes {
				if batchIdx < len(stripeBatches[stripeIdx]) {
					batches = append(batches, stripeBatches[stripeIdx][batchIdx])
				}
			}
		}

		return batches
	})
}

// hashKey produces a uint64 hash for any comparable key using FNV-1a
// over its fmt.Sprint representation.
func hashKey[K comparable](key K) uint64 {
	h := fnv.New64a()
	_, _ = fmt.Fprint(h, key)
	return h.Sum64()
}

// CombineBatchers applies multiple batching strategies in sequence.
// Each batcher processes the output of the previous batcher.
//
// Example:
//
//	// First group by category, then limit each group to 25 items
//	batcher := etl.CombineBatchers(
//		etl.GroupByField(func(t Target) string { return t.Category }),
//		etl.SizeBatcher[Target](25),
//	)
func CombineBatchers[Target any](batchers ...Batcher[Target]) Batcher[Target] {
	return BatcherFunc[Target](func(items []Target) [][]Target {
		current := [][]Target{items}

		for _, batcher := range batchers {
			var next [][]Target
			for _, batch := range current {
				results := batcher.Batch(batch)
				next = append(next, results...)
			}
			current = next
		}

		return current
	})
}

// chunk splits a slice into sub-slices of at most size elements.
func chunk[T any](items []T, size int) [][]T {
	if len(items) == 0 || size <= 0 {
		return nil
	}

	numChunks := (len(items) + size - 1) / size
	result := make([][]T, 0, numChunks)

	for i := 0; i < len(items); i += size {
		end := min(i+size, len(items))
		result = append(result, items[i:end])
	}

	return result
}

// groupBy groups items by a key extracted from each item.
func groupBy[T any, K comparable](items []T, keyFn func(T) K) map[K][]T {
	result := make(map[K][]T)
	for _, item := range items {
		key := keyFn(item)
		result[key] = append(result[key], item)
	}
	return result
}
