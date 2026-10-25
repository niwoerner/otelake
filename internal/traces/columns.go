package traces

import (
	"errors"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/xpdata"
	"go.opentelemetry.io/collector/pdata/xpdata/entity"
)

var jsonMarshaler xpdata.JSONMarshaler

// columns appends one row field by field, in schema order. Every append names
// its column, so code that drifts from the schema panics instead of silently
// writing values into the wrong column.
type columns struct {
	builders []array.Builder
	fields   []arrow.Field
	i        int
	err      error
	lists    map[string]*list
}

func newColumns(builders []array.Builder, fields []arrow.Field) *columns {
	return &columns{builders: builders, fields: fields, lists: map[string]*list{}}
}

func (c *columns) reset() {
	c.i = 0
	c.err = nil
}

// done checks that the row filled every column and reports append errors.
func (c *columns) done() error {
	if c.i != len(c.fields) {
		panic(fmt.Sprintf("traces: row filled %d of %d columns", c.i, len(c.fields)))
	}
	err := c.err
	for _, l := range c.lists {
		if l.started && l.i != len(l.fields) {
			panic(fmt.Sprintf("traces: list element filled %d of %d fields", l.i, len(l.fields)))
		}
		err = errors.Join(err, l.err)
		l.err = nil
	}
	return err
}

func (c *columns) next(name string) array.Builder {
	if c.i >= len(c.fields) || c.fields[c.i].Name != name {
		panic(fmt.Sprintf("traces: wrote column %q at position %d, schema differs", name, c.i))
	}
	b := c.builders[c.i]
	c.i++
	return b
}

func (c *columns) fail(err error) {
	if err != nil && c.err == nil {
		c.err = err
	}
}

func (c *columns) dict(name, v string) {
	c.fail(c.next(name).(*array.BinaryDictionaryBuilder).AppendString(v))
}

func (c *columns) str(name, v string) {
	c.next(name).(*array.StringBuilder).Append(v)
}

func (c *columns) u32(name string, v uint32) {
	c.next(name).(*array.Uint32Builder).Append(v)
}

func (c *columns) i8(name string, v int8) {
	c.next(name).(*array.Int8Builder).Append(v)
}

func (c *columns) i64(name string, v int64) {
	c.next(name).(*array.Int64Builder).Append(v)
}

func (c *columns) timestamp(name string, v pcommon.Timestamp) {
	c.next(name).(*array.TimestampBuilder).Append(arrow.Timestamp(v))
}

// traceID and spanID store empty IDs as null.
func (c *columns) traceID(name string, id pcommon.TraceID) {
	b := c.next(name).(*array.FixedSizeBinaryBuilder)
	if id.IsEmpty() {
		b.AppendNull()
		return
	}
	b.Append(id[:])
}

func (c *columns) spanID(name string, id pcommon.SpanID) {
	b := c.next(name).(*array.FixedSizeBinaryBuilder)
	if id.IsEmpty() {
		b.AppendNull()
		return
	}
	b.Append(id[:])
}

// attrs fills the six typed attribute map columns of one attribute list.
func (c *columns) attrs(prefix string, m pcommon.Map) {
	strs := c.next(prefix + "attrs_str").(*array.MapBuilder)
	ints := c.next(prefix + "attrs_int").(*array.MapBuilder)
	floats := c.next(prefix + "attrs_f64").(*array.MapBuilder)
	bools := c.next(prefix + "attrs_bool").(*array.MapBuilder)
	bytes := c.next(prefix + "attrs_bytes").(*array.MapBuilder)
	anys := c.next(prefix + "attrs_any").(*array.MapBuilder)
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
			c.fail(err)
			key(anys, k).(*array.StringBuilder).Append(string(js))
		}
	}
}

// key appends a map key and returns the builder for its value.
func key(mb *array.MapBuilder, k string) array.Builder {
	mb.KeyBuilder().(*array.StringBuilder).Append(k)
	return mb.ItemBuilder()
}

func (c *columns) entityRefs(name string, refs entity.EntityRefSlice) {
	l := c.list(name)
	for _, ref := range refs.All() {
		l.next()
		l.str("schema_url", ref.SchemaUrl())
		l.str("type", ref.Type())
		l.strs("id_keys", ref.IdKeys())
		l.strs("description_keys", ref.DescriptionKeys())
	}
}

func (c *columns) strs(name string, s pcommon.StringSlice) {
	lb := c.next(name).(*array.ListBuilder)
	lb.Append(true)
	vb := lb.ValueBuilder().(*array.StringBuilder)
	for _, v := range s.All() {
		vb.Append(v)
	}
}

// list starts a list-of-struct value; call next on the result per element.
func (c *columns) list(name string) *list {
	lb := c.next(name).(*array.ListBuilder)
	lb.Append(true)
	l, ok := c.lists[name]
	if !ok {
		sb := lb.ValueBuilder().(*array.StructBuilder)
		builders := make([]array.Builder, sb.NumField())
		for i := range builders {
			builders[i] = sb.FieldBuilder(i)
		}
		fields := sb.Type().(*arrow.StructType).Fields()
		l = &list{columns: newColumns(builders, fields), sb: sb}
		c.lists[name] = l
	}
	l.started = false
	return l
}

// list fills the struct elements of one list value.
type list struct {
	*columns
	sb      *array.StructBuilder
	started bool
}

// next starts the next element, checking that the previous one is complete.
func (l *list) next() {
	if l.started && l.i != len(l.fields) {
		panic(fmt.Sprintf("traces: list element filled %d of %d fields", l.i, len(l.fields)))
	}
	l.started = true
	l.sb.Append(true)
	l.i = 0
}
