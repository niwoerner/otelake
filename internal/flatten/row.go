package flatten

import (
	"errors"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/xpdata"
)

var jsonMarshaler xpdata.JSONMarshaler

// Row appends one row field by field, in schema order. Every append names its
// column, so code that drifts from the schema panics instead of silently
// writing values into the wrong column.
type Row struct {
	builders []array.Builder
	fields   []arrow.Field
	i        int
	err      error
	lists    map[string]*List
}

// NewRow appends rows to the columns of b.
func NewRow(b *array.RecordBuilder) *Row {
	return newRow(b.Fields(), b.Schema().Fields())
}

func newRow(builders []array.Builder, fields []arrow.Field) *Row {
	return &Row{builders: builders, fields: fields, lists: map[string]*List{}}
}

// Reset starts the next row.
func (r *Row) Reset() {
	r.i = 0
	r.err = nil
}

// Done checks that the row filled every column and reports append errors.
func (r *Row) Done() error {
	if r.i != len(r.fields) {
		panic(fmt.Sprintf("flatten: row filled %d of %d columns", r.i, len(r.fields)))
	}
	err := r.err
	for _, l := range r.lists {
		if l.started && l.i != len(l.fields) {
			panic(fmt.Sprintf("flatten: list element filled %d of %d fields", l.i, len(l.fields)))
		}
		err = errors.Join(err, l.err)
		l.err = nil
	}
	return err
}

func (r *Row) next(name string) array.Builder {
	if r.i >= len(r.fields) || r.fields[r.i].Name != name {
		panic(fmt.Sprintf("flatten: wrote column %q at position %d, schema differs", name, r.i))
	}
	b := r.builders[r.i]
	r.i++
	return b
}

func (r *Row) fail(err error) {
	if err != nil && r.err == nil {
		r.err = err
	}
}

// Null appends null to a nullable column of any type.
func (r *Row) Null(name string) {
	r.next(name).AppendNull()
}

// Nulls appends null to several nullable columns, in schema order.
func (r *Row) Nulls(names ...string) {
	for _, name := range names {
		r.Null(name)
	}
}

func (r *Row) Dict(name, v string) {
	r.fail(r.next(name).(*array.BinaryDictionaryBuilder).AppendString(v))
}

func (r *Row) Str(name, v string) {
	r.next(name).(*array.StringBuilder).Append(v)
}

func (r *Row) Bool(name string, v bool) {
	r.next(name).(*array.BooleanBuilder).Append(v)
}

func (r *Row) I8(name string, v int8) {
	r.next(name).(*array.Int8Builder).Append(v)
}

func (r *Row) I32(name string, v int32) {
	r.next(name).(*array.Int32Builder).Append(v)
}

func (r *Row) I64(name string, v int64) {
	r.next(name).(*array.Int64Builder).Append(v)
}

func (r *Row) U32(name string, v uint32) {
	r.next(name).(*array.Uint32Builder).Append(v)
}

func (r *Row) U64(name string, v uint64) {
	r.next(name).(*array.Uint64Builder).Append(v)
}

func (r *Row) F64(name string, v float64) {
	r.next(name).(*array.Float64Builder).Append(v)
}

func (r *Row) Timestamp(name string, v pcommon.Timestamp) {
	r.next(name).(*array.TimestampBuilder).Append(arrow.Timestamp(v))
}

// TraceID and SpanID store empty IDs as null.
func (r *Row) TraceID(name string, id pcommon.TraceID) {
	b := r.next(name).(*array.FixedSizeBinaryBuilder)
	if id.IsEmpty() {
		b.AppendNull()
		return
	}
	b.Append(id[:])
}

func (r *Row) SpanID(name string, id pcommon.SpanID) {
	b := r.next(name).(*array.FixedSizeBinaryBuilder)
	if id.IsEmpty() {
		b.AppendNull()
		return
	}
	b.Append(id[:])
}

// JSON stores a value as OTLP JSON, which keeps nested value types.
func (r *Row) JSON(name string, v pcommon.Value) {
	js, err := jsonMarshaler.MarshalValue(v)
	r.fail(err)
	r.next(name).(*array.StringBuilder).Append(string(js))
}

// Attrs fills the six typed attribute map columns of one attribute list.
func (r *Row) Attrs(prefix string, m pcommon.Map) {
	strs := r.next(prefix + "attrs_str").(*array.MapBuilder)
	ints := r.next(prefix + "attrs_int").(*array.MapBuilder)
	floats := r.next(prefix + "attrs_f64").(*array.MapBuilder)
	bools := r.next(prefix + "attrs_bool").(*array.MapBuilder)
	bytes := r.next(prefix + "attrs_bytes").(*array.MapBuilder)
	anys := r.next(prefix + "attrs_any").(*array.MapBuilder)
	for _, mb := range [...]*array.MapBuilder{strs, ints, floats, bools, bytes, anys} {
		mb.Append(true)
	}

	for k, v := range m.All() {
		switch v.Type() {
		case pcommon.ValueTypeStr:
			key(strs, k).(*array.StringBuilder).Append(v.Str())
		case pcommon.ValueTypeInt:
			key(ints, k).(*array.Int64Builder).Append(v.Int())
		case pcommon.ValueTypeDouble:
			key(floats, k).(*array.Float64Builder).Append(v.Double())
		case pcommon.ValueTypeBool:
			key(bools, k).(*array.BooleanBuilder).Append(v.Bool())
		case pcommon.ValueTypeBytes:
			key(bytes, k).(*array.BinaryBuilder).Append(v.Bytes().AsRaw())
		default: // arrays, kvlists and empty values as OTLP JSON
			js, err := jsonMarshaler.MarshalValue(v)
			r.fail(err)
			key(anys, k).(*array.StringBuilder).Append(string(js))
		}
	}
}

// key appends a map key and returns the builder for its value.
func key(mb *array.MapBuilder, k string) array.Builder {
	mb.KeyBuilder().(*array.StringBuilder).Append(k)
	return mb.ItemBuilder()
}

func (r *Row) Strs(name string, s pcommon.StringSlice) {
	vb := r.listValues(name).(*array.StringBuilder)
	for _, v := range s.All() {
		vb.Append(v)
	}
}

func (r *Row) U64s(name string, s pcommon.UInt64Slice) {
	vb := r.listValues(name).(*array.Uint64Builder)
	for _, v := range s.All() {
		vb.Append(v)
	}
}

func (r *Row) F64s(name string, s pcommon.Float64Slice) {
	vb := r.listValues(name).(*array.Float64Builder)
	for _, v := range s.All() {
		vb.Append(v)
	}
}

// listValues starts a list value and returns the builder of its elements.
func (r *Row) listValues(name string) array.Builder {
	lb := r.next(name).(*array.ListBuilder)
	lb.Append(true)
	return lb.ValueBuilder()
}

// List starts a list-of-struct value; call Next on the result per element.
func (r *Row) List(name string) *List {
	lb := r.next(name).(*array.ListBuilder)
	lb.Append(true)
	l, ok := r.lists[name]
	if !ok {
		sb := lb.ValueBuilder().(*array.StructBuilder)
		builders := make([]array.Builder, sb.NumField())
		for i := range builders {
			builders[i] = sb.FieldBuilder(i)
		}
		l = &List{Row: newRow(builders, sb.Type().(*arrow.StructType).Fields()), sb: sb}
		r.lists[name] = l
	}
	l.started = false
	return l
}

// List fills the struct elements of one list value.
type List struct {
	*Row
	sb      *array.StructBuilder
	started bool
}

// Next starts the next element, checking that the previous one is complete.
func (l *List) Next() {
	if l.started && l.i != len(l.fields) {
		panic(fmt.Sprintf("flatten: list element filled %d of %d fields", l.i, len(l.fields)))
	}
	l.started = true
	l.sb.Append(true)
	l.i = 0
}
