package etl

import (
	"encoding/json"
	"log/slog"
	"sync/atomic"
)

// Stats provides pipeline statistics with thread-safe access.
// Counter fields use atomic operations for safe concurrent access from worker goroutines.
type Stats struct {
	extracted   atomic.Int64
	filtered    atomic.Int64
	transformed atomic.Int64
	loaded      atomic.Int64
	errors      atomic.Int64
}

// NewStats creates a Stats with initial counter values.
// Use this when loading checkpoint data from storage.
func NewStats(extracted, filtered, transformed, loaded, errors int64) *Stats {
	s := &Stats{}
	s.extracted.Store(extracted)
	s.filtered.Store(filtered)
	s.transformed.Store(transformed)
	s.loaded.Store(loaded)
	s.errors.Store(errors)
	return s
}

// Extracted returns the number of records extracted.
func (s *Stats) Extracted() int64 { return s.extracted.Load() }

// Filtered returns the number of records filtered out before transformation.
func (s *Stats) Filtered() int64 { return s.filtered.Load() }

// Transformed returns the number of records transformed.
func (s *Stats) Transformed() int64 { return s.transformed.Load() }

// Loaded returns the number of records loaded.
func (s *Stats) Loaded() int64 { return s.loaded.Load() }

// Errors returns the number of errors encountered.
func (s *Stats) Errors() int64 { return s.errors.Load() }

// LogValue implements slog.LogValuer for structured logging.
func (s *Stats) LogValue() slog.Value {
	return slog.GroupValue(
		slog.Int64("extracted", s.Extracted()),
		slog.Int64("filtered", s.Filtered()),
		slog.Int64("transformed", s.Transformed()),
		slog.Int64("loaded", s.Loaded()),
		slog.Int64("errors", s.Errors()),
	)
}

// statsJSON is the JSON representation for marshaling/unmarshaling Stats.
type statsJSON struct {
	Extracted   int64 `json:"extracted"`
	Filtered    int64 `json:"filtered"`
	Transformed int64 `json:"transformed"`
	Loaded      int64 `json:"loaded"`
	Errors      int64 `json:"errors"`
}

// MarshalJSON implements json.Marshaler for Stats serialization.
func (s *Stats) MarshalJSON() ([]byte, error) {
	return json.Marshal(statsJSON{
		Extracted:   s.extracted.Load(),
		Filtered:    s.filtered.Load(),
		Transformed: s.transformed.Load(),
		Loaded:      s.loaded.Load(),
		Errors:      s.errors.Load(),
	})
}

// UnmarshalJSON implements json.Unmarshaler for Stats deserialization.
func (s *Stats) UnmarshalJSON(data []byte) error {
	var v statsJSON
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	s.extracted.Store(v.Extracted)
	s.filtered.Store(v.Filtered)
	s.transformed.Store(v.Transformed)
	s.loaded.Store(v.Loaded)
	s.errors.Store(v.Errors)
	return nil
}

// Internal increment methods. These return the new value after incrementing,
// which is essential for race-free progress tracking across concurrent workers.
func (s *Stats) incExtracted(n int64) int64   { return s.extracted.Add(n) }
func (s *Stats) incFiltered(n int64) int64    { return s.filtered.Add(n) }
func (s *Stats) incTransformed(n int64) int64 { return s.transformed.Add(n) }
func (s *Stats) incLoaded(n int64) int64      { return s.loaded.Add(n) }
func (s *Stats) incErrors(n int64) int64      { return s.errors.Add(n) }
