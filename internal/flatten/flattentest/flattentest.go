// Package flattentest reads values back out of flattened Arrow records in tests.
package flattentest

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Allocator returns an allocator that fails the test if memory leaks.
func Allocator(t testing.TB) memory.Allocator {
	t.Helper()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { mem.AssertSize(t, 0) })
	return mem
}

// Column returns the named column as T.
func Column[T arrow.Array](t testing.TB, rec arrow.RecordBatch, name string) T {
	t.Helper()
	idx := rec.Schema().FieldIndices(name)
	if len(idx) == 0 {
		t.Fatalf("no column %q", name)
	}
	return rec.Column(idx[0]).(T)
}

// Dict returns one value of a dictionary-encoded string column.
func Dict(t testing.TB, rec arrow.RecordBatch, name string, row int) string {
	t.Helper()
	d := Column[*array.Dictionary](t, rec, name)
	return d.Dictionary().(*array.String).Value(d.GetValueIndex(row))
}

// Map returns the key/value pairs of one row of a map column.
func Map(t testing.TB, rec arrow.RecordBatch, name string, row int) map[string]any {
	t.Helper()
	m := Column[*array.Map](t, rec, name)
	start, end := m.ValueOffsets(row)
	keys := m.Keys().(*array.String)
	entries := map[string]any{}
	for i := int(start); i < int(end); i++ {
		entries[keys.Value(i)] = m.Items().GetOneForMarshal(i)
	}
	return entries
}

// Len returns the number of elements of one row of a list column.
func Len(t testing.TB, rec arrow.RecordBatch, name string, row int) int {
	t.Helper()
	start, end := Column[*array.List](t, rec, name).ValueOffsets(row)
	return int(end - start)
}
