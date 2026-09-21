package capability

import (
	"fmt"

	"github.com/domainry/domainry-scheduler-sdk/authoring"
	capabilitycontract "github.com/domainry/domainry-scheduler-sdk/authoring/contract"
)

// schedulerBlueprintSourceProjectionKind deliberately differs from the
// Scheduler SDK management authoring contract. The latter describes the
// flattened definition record accepted by Scheduler's management HTTP API;
// this projection describes the nested Domain Blueprint source fragment that
// ValidateCandidate accepts before Plane compiles it to that runtime record.
const schedulerBlueprintSourceProjectionKind = "scheduler.blueprint_source_capability"

func schedulerBlueprintSourceCapability() (capabilitycontract.CapabilityAuthoringDefinition, error) {
	management, err := schedulerManagementBusinessJobCapability()
	if err != nil {
		return capabilitycontract.CapabilityAuthoringDefinition{}, err
	}
	properties := management.InputSchema.Properties
	for _, key := range []string{"key", "name", "status", "target_type", "target_key", "target_object", "run_as_role", "connection_key", "payload_json", "schedule_type", "schedule_expression", "interval_seconds", "time_of_day", "day_of_week", "day_of_month", "timezone", "business_calendar_key", "non_working_day_policy", "missed_window_policy", "max_catchup_windows", "max_attempts", "retry_delay_seconds", "retry_max_delay_seconds", "timeout_seconds", "description", "i18n"} {
		if _, exists := properties[key]; !exists {
			return capabilitycontract.CapabilityAuthoringDefinition{}, fmt.Errorf("Scheduler management business job schema has no %q property", key)
		}
	}
	property := func(key string) capabilitycontract.CapabilityAuthoringSchema {
		return properties[key]
	}

	status := property("status")
	targetType := property("target_type")

	scheduleProperties := map[string]capabilitycontract.CapabilityAuthoringSchema{
		"expression":       property("schedule_expression"),
		"interval_seconds": property("interval_seconds"),
		"time_of_day":      property("time_of_day"),
		"day_of_week":      property("day_of_week"),
		"day_of_month":     property("day_of_month"),
		"timezone":         property("timezone"),
	}
	closed := false
	variant := func(fields ...string) capabilitycontract.CapabilityAuthoringSchema {
		selected := map[string]capabilitycontract.CapabilityAuthoringSchema{}
		for _, field := range fields {
			selected[field] = scheduleProperties[field]
		}
		return capabilitycontract.CapabilityAuthoringSchema{
			Type: "object", AdditionalProperties: &closed, Properties: selected,
			Required: append([]string(nil), fields...),
		}
	}
	schedule := capabilitycontract.CapabilityAuthoringSchema{OneOf: []capabilitycontract.CapabilityAuthoringSchema{
		variant("expression", "timezone"),
		variant("interval_seconds"),
		variant("time_of_day", "timezone"),
		variant("time_of_day", "day_of_week", "timezone"),
		variant("time_of_day", "day_of_month", "timezone"),
	}}
	// Timezone is optional for calendar schedules and defaults to UTC in the
	// owner validator. Remove it from each variant's required set while keeping
	// it in the closed property set.
	for index := range schedule.OneOf {
		required := schedule.OneOf[index].Required[:0]
		for _, field := range schedule.OneOf[index].Required {
			if field != "timezone" {
				required = append(required, field)
			}
		}
		schedule.OneOf[index].Required = required
	}

	schema := &capabilitycontract.CapabilityAuthoringSchema{
		Schema: "https://json-schema.org/draft/2020-12/schema", Type: "object", AdditionalProperties: &closed,
		Properties: map[string]capabilitycontract.CapabilityAuthoringSchema{
			"key":                     property("key"),
			"name":                    property("name"),
			"status":                  status,
			"target_type":             targetType,
			"target_key":              property("target_key"),
			"target_object":           property("target_object"),
			"run_as_role":             property("run_as_role"),
			"connection_key":          property("connection_key"),
			"payload_json":            property("payload_json"),
			"schedule":                schedule,
			"business_calendar_key":   property("business_calendar_key"),
			"non_working_day_policy":  property("non_working_day_policy"),
			"missed_window_policy":    property("missed_window_policy"),
			"max_catchup_windows":     property("max_catchup_windows"),
			"max_attempts":            property("max_attempts"),
			"retry_delay_seconds":     property("retry_delay_seconds"),
			"retry_max_delay_seconds": property("retry_max_delay_seconds"),
			"timeout_seconds":         property("timeout_seconds"),
			"description":             property("description"),
			"i18n":                    property("i18n"),
		},
		Required: []string{"key", "name", "status", "target_type", "target_key", "schedule", "missed_window_policy", "max_attempts", "timeout_seconds"},
	}
	return capabilitycontract.CapabilityAuthoringDefinition{
		Key: "scheduler.business_job", Status: "supported", Lifecycle: "domain_blueprint_source_fragment",
		InputSchema: schema,
		ReferenceContracts: []capabilitycontract.CapabilityAuthoringReference{
			{Kind: "scheduler_target_key", InputJSONPointer: "/target_key", ScopeFrom: "/target_type", ResolverEndpoint: "/discovery/references/scheduler_target_key"},
			{Kind: "object_key", InputJSONPointer: "/target_object", ResolverEndpoint: "/discovery/references/object_key"},
			{Kind: "role_key", InputJSONPointer: "/run_as_role", ResolverEndpoint: "/discovery/references/role_key"},
			{Kind: "connection_key", InputJSONPointer: "/connection_key", ResolverEndpoint: "/discovery/references/connection_key"},
			{Kind: "business_calendar_key", InputJSONPointer: "/business_calendar_key", ResolverEndpoint: "/discovery/references/business_calendar_key"},
		},
		Examples: []capabilitycontract.CapabilityAuthoringExample{
			{Name: "minimal_valid", Value: map[string]any{"key": "daily_operations_snapshot", "name": "Daily operations snapshot", "status": "enabled", "target_type": "report_snapshot_refresh", "target_key": "orders.daily", "schedule": map[string]any{"time_of_day": "02:00", "timezone": "UTC"}, "missed_window_policy": "skip", "max_attempts": 1, "timeout_seconds": 300}},
			{Name: "representative", Value: map[string]any{"key": "order_expiration", "name": "Expire overdue orders", "status": "enabled", "target_type": "business_action", "target_key": "order.expire_overdue", "target_object": "order", "run_as_role": "order_automation", "payload_json": "{\"status\":\"overdue\"}", "schedule": map[string]any{"expression": "0 9 * * *", "timezone": "Asia/Shanghai"}, "business_calendar_key": "cn_operations", "non_working_day_policy": "roll_forward", "missed_window_policy": "catch_up_bounded", "max_catchup_windows": 3, "max_attempts": 3, "retry_delay_seconds": 10, "retry_max_delay_seconds": 300, "timeout_seconds": 1800}},
			{Name: "http_target", Value: map[string]any{"key": "partner_sync", "name": "Partner sync", "status": "enabled", "target_type": "http", "target_key": "sync_orders", "connection_key": "partner_api", "payload_json": "{\"mode\":\"incremental\"}", "schedule": map[string]any{"interval_seconds": 900}, "missed_window_policy": "skip", "max_attempts": 3, "timeout_seconds": 120}},
		},
		Sources: []capabilitycontract.CapabilityAuthoringSource{{Kind: "owner_module", Path: "github.com/domainry/domainry-scheduler/capability", Symbol: "scheduledJobSource"}},
	}, nil
}

func schedulerManagementBusinessJobCapability() (capabilitycontract.CapabilityAuthoringDefinition, error) {
	for _, capability := range authoring.ManagementDomain().Capabilities {
		if capability.Key == "scheduler.business_job" {
			if capability.InputSchema == nil {
				return capabilitycontract.CapabilityAuthoringDefinition{}, fmt.Errorf("Scheduler management business job input schema is absent")
			}
			return capability, nil
		}
	}
	return capabilitycontract.CapabilityAuthoringDefinition{}, fmt.Errorf("Scheduler management business job capability is absent")
}
