package etl_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bjaus/etl"
)

func TestSizeBatcher(t *testing.T) {
	tests := []struct {
		name     string
		maxSize  int
		items    []string
		expected [][]string
	}{
		{
			name:     "empty items",
			maxSize:  3,
			items:    []string{},
			expected: nil,
		},
		{
			name:     "zero max size",
			maxSize:  0,
			items:    []string{"a", "b", "c"},
			expected: nil,
		},
		{
			name:     "negative max size",
			maxSize:  -1,
			items:    []string{"a", "b", "c"},
			expected: nil,
		},
		{
			name:     "items fit in one batch",
			maxSize:  5,
			items:    []string{"a", "b", "c"},
			expected: [][]string{{"a", "b", "c"}},
		},
		{
			name:     "items require multiple batches",
			maxSize:  2,
			items:    []string{"a", "b", "c", "d", "e"},
			expected: [][]string{{"a", "b"}, {"c", "d"}, {"e"}},
		},
		{
			name:     "exact batch size",
			maxSize:  3,
			items:    []string{"a", "b", "c", "d", "e", "f"},
			expected: [][]string{{"a", "b", "c"}, {"d", "e", "f"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batcher := etl.SizeBatcher[string](tt.maxSize)
			result := batcher.Batch(tt.items)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestGroupByField(t *testing.T) {
	type TestRecord struct {
		ID     string
		UserID string
		Value  string
	}

	tests := []struct {
		name     string
		items    []TestRecord
		expected int // number of batches (since map iteration order is not deterministic)
	}{
		{
			name:     "empty items",
			items:    []TestRecord{},
			expected: 0,
		},
		{
			name: "single group",
			items: []TestRecord{
				{ID: "1", UserID: "user1", Value: "a"},
				{ID: "2", UserID: "user1", Value: "b"},
				{ID: "3", UserID: "user1", Value: "c"},
			},
			expected: 1,
		},
		{
			name: "multiple groups",
			items: []TestRecord{
				{ID: "1", UserID: "user1", Value: "a"},
				{ID: "2", UserID: "user2", Value: "b"},
				{ID: "3", UserID: "user1", Value: "c"},
				{ID: "4", UserID: "user3", Value: "d"},
				{ID: "5", UserID: "user2", Value: "e"},
			},
			expected: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batcher := etl.GroupByField(func(r TestRecord) string {
				return r.UserID
			})
			result := batcher.Batch(tt.items)

			if tt.expected == 0 {
				require.Nil(t, result)
			} else {
				require.Len(t, result, tt.expected)

				// Verify all items are still present
				totalItems := 0
				for _, batch := range result {
					totalItems += len(batch)
					// Verify all items in a batch have the same UserID
					if len(batch) > 0 {
						userID := batch[0].UserID
						for _, item := range batch {
							require.Equal(t, userID, item.UserID)
						}
					}
				}
				require.Equal(t, len(tt.items), totalItems)
			}
		})
	}
}

func TestGroupByFieldWithSizeLimit(t *testing.T) {
	type TestRecord struct {
		ID     string
		UserID string
		Value  string
	}

	tests := []struct {
		name         string
		items        []TestRecord
		maxGroupSize int
	}{
		{
			name:         "empty items",
			items:        []TestRecord{},
			maxGroupSize: 2,
		},
		{
			name: "zero max group size",
			items: []TestRecord{
				{ID: "1", UserID: "user1", Value: "a"},
			},
			maxGroupSize: 0,
		},
		{
			name: "negative max group size",
			items: []TestRecord{
				{ID: "1", UserID: "user1", Value: "a"},
			},
			maxGroupSize: -1,
		},
		{
			name: "group fits within size limit",
			items: []TestRecord{
				{ID: "1", UserID: "user1", Value: "a"},
				{ID: "2", UserID: "user1", Value: "b"},
				{ID: "3", UserID: "user2", Value: "c"},
			},
			maxGroupSize: 3,
		},
		{
			name: "group exceeds size limit",
			items: []TestRecord{
				{ID: "1", UserID: "user1", Value: "a"},
				{ID: "2", UserID: "user1", Value: "b"},
				{ID: "3", UserID: "user1", Value: "c"},
				{ID: "4", UserID: "user1", Value: "d"},
				{ID: "5", UserID: "user2", Value: "e"},
			},
			maxGroupSize: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batcher := etl.GroupByFieldWithSizeLimit(
				func(r TestRecord) string { return r.UserID },
				tt.maxGroupSize,
			)
			result := batcher.Batch(tt.items)

			if len(tt.items) == 0 || tt.maxGroupSize <= 0 {
				require.Nil(t, result)
				return
			}

			require.NotNil(t, result)

			// Verify all batches respect size limit
			for _, batch := range result {
				require.LessOrEqual(t, len(batch), tt.maxGroupSize)
			}

			// Verify all items are still present
			totalItems := 0
			for _, batch := range result {
				totalItems += len(batch)
			}
			require.Equal(t, len(tt.items), totalItems)
		})
	}
}

func TestCombineBatchers(t *testing.T) {
	type TestRecord struct {
		ID     string
		UserID string
		Value  string
	}

	t.Run("empty batchers list", func(t *testing.T) {
		items := []TestRecord{
			{ID: "1", UserID: "user1", Value: "a"},
		}

		batcher := etl.CombineBatchers[TestRecord]()
		result := batcher.Batch(items)

		require.Equal(t, [][]TestRecord{items}, result)
	})

	t.Run("single batcher", func(t *testing.T) {
		items := []TestRecord{
			{ID: "1", UserID: "user1", Value: "a"},
			{ID: "2", UserID: "user1", Value: "b"},
			{ID: "3", UserID: "user2", Value: "c"},
		}

		sizeBatcher := etl.SizeBatcher[TestRecord](2)
		combinedBatcher := etl.CombineBatchers(sizeBatcher)
		result := combinedBatcher.Batch(items)

		require.Len(t, result, 2)
		require.Len(t, result[0], 2)
		require.Len(t, result[1], 1)
	})

	t.Run("multiple batchers", func(t *testing.T) {
		items := []TestRecord{
			{ID: "1", UserID: "user1", Value: "a"},
			{ID: "2", UserID: "user1", Value: "b"},
			{ID: "3", UserID: "user1", Value: "c"},
			{ID: "4", UserID: "user2", Value: "d"},
			{ID: "5", UserID: "user2", Value: "e"},
		}

		groupBatcher := etl.GroupByField(func(r TestRecord) string { return r.UserID })
		sizeBatcher := etl.SizeBatcher[TestRecord](2)
		combinedBatcher := etl.CombineBatchers(groupBatcher, sizeBatcher)

		result := combinedBatcher.Batch(items)

		totalItems := 0
		for _, batch := range result {
			totalItems += len(batch)
			require.LessOrEqual(t, len(batch), 2)
		}
		require.Equal(t, len(items), totalItems)
	})

	t.Run("empty items", func(t *testing.T) {
		var items []TestRecord

		groupBatcher := etl.GroupByField(func(r TestRecord) string { return r.UserID })
		sizeBatcher := etl.SizeBatcher[TestRecord](2)
		combinedBatcher := etl.CombineBatchers(groupBatcher, sizeBatcher)

		result := combinedBatcher.Batch(items)
		require.Nil(t, result)
	})
}

func TestWeightedBatcher(t *testing.T) {
	type record struct {
		ID   string
		Size int
	}

	weigher := func(r record) int { return r.Size }

	t.Run("empty items", func(t *testing.T) {
		batcher := etl.WeightedBatcher(weigher, 100)
		result := batcher.Batch([]record{})
		require.Nil(t, result)
	})

	t.Run("zero max weight", func(t *testing.T) {
		batcher := etl.WeightedBatcher(weigher, 0)
		result := batcher.Batch([]record{{ID: "1", Size: 10}})
		require.Nil(t, result)
	})

	t.Run("negative max weight", func(t *testing.T) {
		batcher := etl.WeightedBatcher(weigher, -1)
		result := batcher.Batch([]record{{ID: "1", Size: 10}})
		require.Nil(t, result)
	})

	t.Run("all items fit in one batch", func(t *testing.T) {
		items := []record{
			{ID: "1", Size: 10},
			{ID: "2", Size: 20},
			{ID: "3", Size: 30},
		}
		batcher := etl.WeightedBatcher(weigher, 100)
		result := batcher.Batch(items)
		require.Len(t, result, 1)
		require.Len(t, result[0], 3)
	})

	t.Run("items split across batches", func(t *testing.T) {
		items := []record{
			{ID: "1", Size: 30},
			{ID: "2", Size: 30},
			{ID: "3", Size: 30},
			{ID: "4", Size: 30},
			{ID: "5", Size: 30},
		}
		batcher := etl.WeightedBatcher(weigher, 70)
		result := batcher.Batch(items)

		// 30+30=60 (fits), 30 would make 90 (over 70), so batch 1 = [1,2]
		// 30+30=60 (fits), 30 would make 90, so batch 2 = [3,4]
		// 30 fits, batch 3 = [5]
		require.Len(t, result, 3)
		require.Len(t, result[0], 2)
		require.Len(t, result[1], 2)
		require.Len(t, result[2], 1)
	})

	t.Run("single item exceeds max weight", func(t *testing.T) {
		items := []record{
			{ID: "1", Size: 10},
			{ID: "2", Size: 200}, // exceeds maxWeight of 100
			{ID: "3", Size: 10},
		}
		batcher := etl.WeightedBatcher(weigher, 100)
		result := batcher.Batch(items)

		// Item 2 should still be included in its own batch, never dropped
		require.Len(t, result, 3)
		require.Equal(t, "1", result[0][0].ID)
		require.Equal(t, "2", result[1][0].ID)
		require.Equal(t, "3", result[2][0].ID)
	})

	t.Run("exact weight boundary", func(t *testing.T) {
		items := []record{
			{ID: "1", Size: 50},
			{ID: "2", Size: 50},
			{ID: "3", Size: 50},
		}
		batcher := etl.WeightedBatcher(weigher, 100)
		result := batcher.Batch(items)

		// 50+50=100 (exactly at limit), next 50 would be 150, so split
		require.Len(t, result, 2)
		require.Len(t, result[0], 2)
		require.Len(t, result[1], 1)
	})

	t.Run("sql parameter count use case", func(t *testing.T) {
		// Simulate 5 columns per row, 65535 param limit
		type row struct {
			Cols int
		}
		items := make([]row, 20000)
		for i := range items {
			items[i] = row{Cols: 5}
		}

		batcher := etl.WeightedBatcher(func(r row) int { return r.Cols }, 65535)
		result := batcher.Batch(items)

		// 65535 / 5 = 13107 rows per batch, so 20000 rows = 2 batches
		require.Len(t, result, 2)
		require.Len(t, result[0], 13107)
		require.Len(t, result[1], 6893)
	})
}

func TestStripeBatcher(t *testing.T) {
	type record struct {
		ID       string
		TenantID string
	}

	keyFn := func(r record) string { return r.TenantID }

	t.Run("empty items", func(t *testing.T) {
		batcher := etl.StripeBatcher(keyFn, 4, 100)
		result := batcher.Batch([]record{})
		require.Nil(t, result)
	})

	t.Run("zero stripes", func(t *testing.T) {
		batcher := etl.StripeBatcher(keyFn, 0, 100)
		result := batcher.Batch([]record{{ID: "1", TenantID: "t1"}})
		require.Nil(t, result)
	})

	t.Run("zero max batch size", func(t *testing.T) {
		batcher := etl.StripeBatcher(keyFn, 4, 0)
		result := batcher.Batch([]record{{ID: "1", TenantID: "t1"}})
		require.Nil(t, result)
	})

	t.Run("single stripe behaves like grouped size batcher", func(t *testing.T) {
		items := []record{
			{ID: "1", TenantID: "t1"},
			{ID: "2", TenantID: "t1"},
			{ID: "3", TenantID: "t2"},
			{ID: "4", TenantID: "t2"},
		}
		batcher := etl.StripeBatcher(keyFn, 1, 100)
		result := batcher.Batch(items)

		// All in one stripe, grouped by tenant — should be one batch
		require.Len(t, result, 1)
		require.Len(t, result[0], 4)
	})

	t.Run("same key always in same stripe", func(t *testing.T) {
		items := []record{
			{ID: "1", TenantID: "t1"},
			{ID: "2", TenantID: "t2"},
			{ID: "3", TenantID: "t1"},
			{ID: "4", TenantID: "t2"},
			{ID: "5", TenantID: "t1"},
		}
		batcher := etl.StripeBatcher(keyFn, 4, 100)
		result := batcher.Batch(items)

		// Verify all items are present
		total := 0
		for _, batch := range result {
			total += len(batch)
		}
		require.Equal(t, 5, total)
	})

	t.Run("max batch size splits within stripe", func(t *testing.T) {
		// Create 10 items for the same tenant
		items := make([]record, 10)
		for i := range items {
			items[i] = record{ID: fmt.Sprintf("%d", i), TenantID: "t1"}
		}

		batcher := etl.StripeBatcher(keyFn, 2, 3)
		result := batcher.Batch(items)

		// All items go to the same stripe (same key), chunked into batches of 3
		// 10 / 3 = 4 batches (3, 3, 3, 1)
		total := 0
		for _, batch := range result {
			require.LessOrEqual(t, len(batch), 3)
			total += len(batch)
		}
		require.Equal(t, 10, total)
	})

	t.Run("interleaving order", func(t *testing.T) {
		// Create items that will deterministically land in different stripes.
		// We'll use many distinct keys so they spread across stripes.
		var items []record
		for i := 0; i < 100; i++ {
			items = append(items, record{
				ID:       fmt.Sprintf("%d", i),
				TenantID: fmt.Sprintf("tenant-%d", i),
			})
		}

		batcher := etl.StripeBatcher(keyFn, 4, 10)
		result := batcher.Batch(items)

		// Verify all items are present
		total := 0
		for _, batch := range result {
			require.LessOrEqual(t, len(batch), 10)
			total += len(batch)
		}
		require.Equal(t, 100, total)

		// Verify we got more than 4 batches (items should spread across stripes)
		require.Greater(t, len(result), 4)
	})

	t.Run("large group exceeding max batch size", func(t *testing.T) {
		// One tenant with many items, another with few
		var items []record
		for i := 0; i < 25; i++ {
			items = append(items, record{ID: fmt.Sprintf("big-%d", i), TenantID: "big-tenant"})
		}
		items = append(items, record{ID: "small-1", TenantID: "small-tenant"})

		batcher := etl.StripeBatcher(keyFn, 2, 10)
		result := batcher.Batch(items)

		total := 0
		for _, batch := range result {
			require.LessOrEqual(t, len(batch), 10)
			total += len(batch)
		}
		require.Equal(t, 26, total)
	})

	t.Run("flush accumulated before large group", func(t *testing.T) {
		// Create items where a small group is followed by a large group in the same stripe.
		// This tests the path where we flush accumulated items before chunking a large group.
		// All items have the same tenant (same stripe), but we process groups in map iteration order.
		// To guarantee the path is hit, we use multiple small groups followed by a large group.
		var items []record

		// Small group 1 (2 items)
		items = append(items, record{ID: "a-1", TenantID: "tenant-a"})
		items = append(items, record{ID: "a-2", TenantID: "tenant-a"})

		// Large group (15 items, exceeds maxBatchSize of 5)
		for i := 0; i < 15; i++ {
			items = append(items, record{ID: fmt.Sprintf("b-%d", i), TenantID: "tenant-b"})
		}

		// Use 1 stripe so all items go to same stripe
		batcher := etl.StripeBatcher(keyFn, 1, 5)
		result := batcher.Batch(items)

		// Verify all items present
		total := 0
		for _, batch := range result {
			require.LessOrEqual(t, len(batch), 5)
			total += len(batch)
		}
		require.Equal(t, 17, total)

		// Should have at least 4 batches: small group (2) + large group chunked (5+5+5)
		require.GreaterOrEqual(t, len(result), 4)
	})
}

func TestNoBatcher(t *testing.T) {
	t.Run("empty items", func(t *testing.T) {
		batcher := etl.NoBatcher[string]()
		result := batcher.Batch([]string{})
		require.Nil(t, result)
	})

	t.Run("with items", func(t *testing.T) {
		items := []string{"a", "b", "c"}
		batcher := etl.NoBatcher[string]()
		result := batcher.Batch(items)
		require.Equal(t, [][]string{items}, result)
	})
}
