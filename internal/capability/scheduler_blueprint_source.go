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
	for _, key := range []string{"key", "name", "status", "target_type", "target_key", "schedule_type", "schedule_expression", "interval_seconds", "time_of_day", "day_of_week", "day_of_month", "timezone", "missed_window_policy", "max_catchup_windows", "max_attempts", "timeout_seconds", "i18n"} {
		if _, exists := properties[key]; !exists {
			return capabilitycontract.CapabilityAuthoringDefinition{}, fmt.Errorf("Scheduler management business job schema has no %q property", key)
		}
	}
	property := func(key string) capabilitycontract.CapabilityAuthoringSchema {
		return properties[key]
	}

	status := property("status")
	status.Enum = []any{"draft", "enabled", "paused", "disabled"}
	targetType := property("target_type")
	targetType.Enum = []any{"workflow", "report_snapshot_refresh"}

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
			"key":                  property("key"),
			"name":                 property("name"),
			"status":               status,
			"target_type":          targetType,
			"target_key":           property("target_key"),
			"schedule":             schedule,
			"missed_window_policy": property("missed_window_policy"),
			"max_catchup_windows":  property("max_catchup_windows"),
			"max_attempts":         property("max_attempts"),
			"timeout_seconds":      property("timeout_seconds"),
			"i18n":                 property("i18n"),
		},
		Required: []string{"key", "status", "target_type", "target_key", "schedule", "missed_window_policy", "max_attempts", "timeout_seconds"},
	}
	return capabilitycontract.CapabilityAuthoringDefinition{
		Key: "scheduler.business_job", Status: "supported", Lifecycle: "domain_blueprint_source_fragment",
		InputSchema: schema,
		ReferenceContracts: []capabilitycontract.CapabilityAuthoringReference{{
			Kind: "scheduler_target_key", InputJSONPointer: "/target_key", ScopeFrom: "/target_type", ResolverEndpoint: "/discovery/references/scheduler_target_key",
		}},
		Examples: []capabilitycontract.CapabilityAuthoringExample{
			{Name: "minimal_valid", Value: map[string]any{"key": "daily_operations_snapshot", "status": "enabled", "target_type": "report_snapshot_refresh", "target_key": "orders.daily", "schedule": map[string]any{"time_of_day": "02:00", "timezone": "UTC"}, "missed_window_policy": "skip", "max_attempts": 1, "timeout_seconds": 300}},
			{Name: "representative", Value: map[string]any{"key": "order_approval_scan", "status": "enabled", "target_type": "workflow", "target_key": "order.approval", "schedule": map[string]any{"expression": "*/15 * * * *", "timezone": "Asia/Shanghai"}, "missed_window_policy": "catch_up_bounded", "max_catchup_windows": 3, "max_attempts": 3, "timeout_seconds": 1800}},
		},
		Sources: []capabilitycontract.CapabilityAuthoringSource{{Kind: "owner_module", Path: "github.com/domainry/domainry-scheduler/internal/capability", Symbol: "scheduledJobSource"}},
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
