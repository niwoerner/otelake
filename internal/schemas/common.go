package schemas

import (
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
)

// Version is stored in every schema's metadata. Bump it on every change.
// Allowed changes: add nullable columns. Never rename, retype or change meaning.
const Version = "1"

var (
	str        = arrow.BinaryTypes.String
	u32        = arrow.PrimitiveTypes.Uint32
	dictString = &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String}
	timestamp  = &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}
	// Null when the ID is empty; pdata rejects IDs of any other length.
	traceID = &arrow.FixedSizeBinaryType{ByteWidth: 16}
	spanID  = &arrow.FixedSizeBinaryType{ByteWidth: 8}
)

// Columns every signal carries: resource and scope copied onto each row.
func resourceAndScope() []arrow.Field {
	return slices.Concat(
		[]arrow.Field{
			{Name: "service_name", Type: dictString}, // derived from the service.name resource attribute
			{Name: "resource_schema_url", Type: dictString},
		},
		attributes("resource_"),
		[]arrow.Field{
			{Name: "resource_dropped_attributes_count", Type: u32},
			{Name: "resource_entity_refs", Type: arrow.ListOf(arrow.StructOf(
				arrow.Field{Name: "schema_url", Type: str},
				arrow.Field{Name: "type", Type: str},
				arrow.Field{Name: "id_keys", Type: arrow.ListOf(str)},
				arrow.Field{Name: "description_keys", Type: arrow.ListOf(str)},
			))},
			{Name: "scope_schema_url", Type: dictString},
			{Name: "scope_name", Type: dictString},
			{Name: "scope_version", Type: dictString},
		},
		attributes("scope_"),
		[]arrow.Field{
			{Name: "scope_dropped_attributes_count", Type: u32},
		},
	)
}

// attributes stores an OTLP attribute list split by value type, so numbers
// and booleans stay queryable as such. Arrays and kvlists are stored as OTLP
// JSON in attrs_any, which keeps their nested value types.
func attributes(prefix string) []arrow.Field {
	attr := func(name string, value arrow.DataType) arrow.Field {
		return arrow.Field{Name: prefix + name, Type: arrow.MapOf(str, value)}
	}
	return []arrow.Field{
		attr("attrs_str", str),
		attr("attrs_int", arrow.PrimitiveTypes.Int64),
		attr("attrs_f64", arrow.PrimitiveTypes.Float64),
		attr("attrs_bool", arrow.FixedWidthTypes.Boolean),
		attr("attrs_bytes", arrow.BinaryTypes.Binary),
		attr("attrs_any", str),
	}
}

func otlpArrowSchema(fields ...[]arrow.Field) *arrow.Schema {
	md := arrow.NewMetadata([]string{"otelake.schema_version"}, []string{Version})
	return arrow.NewSchema(slices.Concat(fields...), &md)
}
