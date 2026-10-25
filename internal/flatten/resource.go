package flatten

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/xpdata/entity"
)

const UnknownService = "unknown_service"

// ServiceName returns the service.name resource attribute, or UnknownService.
func ServiceName(res pcommon.Resource) string {
	if v, ok := res.Attributes().Get("service.name"); ok && v.Type() == pcommon.ValueTypeStr {
		return v.Str()
	}
	return UnknownService
}

// ResourceAndScope fills the resource and scope columns every signal shares.
func (r *Row) ResourceAndScope(service string, res pcommon.Resource, resourceSchemaURL string, scope pcommon.InstrumentationScope, scopeSchemaURL string) {
	r.Dict("service_name", service)
	r.Dict("resource_schema_url", resourceSchemaURL)
	r.Attrs("resource_", res.Attributes())
	r.U32("resource_dropped_attributes_count", res.DroppedAttributesCount())

	refs := r.List("resource_entity_refs")
	for _, ref := range entity.ResourceEntityRefs(res).All() {
		refs.Next()
		refs.Str("schema_url", ref.SchemaUrl())
		refs.Str("type", ref.Type())
		refs.Strs("id_keys", ref.IdKeys())
		refs.Strs("description_keys", ref.DescriptionKeys())
	}

	r.Dict("scope_schema_url", scopeSchemaURL)
	r.Dict("scope_name", scope.Name())
	r.Dict("scope_version", scope.Version())
	r.Attrs("scope_", scope.Attributes())
	r.U32("scope_dropped_attributes_count", scope.DroppedAttributesCount())
}
