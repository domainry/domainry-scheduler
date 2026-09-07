package capability

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	actioncontract "github.com/domainry/domainry-foundation/action"
	"github.com/domainry/domainry-foundation/modulecapability"
	"github.com/domainry/domainry-foundation/modulehttp"
	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/schedule"
)

const (
	SchedulerAuthoringCategory  = schedulersdk.CapabilitySchedulerAuthoring
	SchedulerOperationsCategory = schedulersdk.CapabilitySchedulerOperations
)

func NewBinding() (*modulecapability.StaticBinding, error) {
	routes, operations, err := schedulerHTTPContract()
	if err != nil {
		return nil, err
	}
	groups := map[string][]modulehttp.Route{}
	for _, route := range routes {
		groups[route.Action.CapabilityKey] = append(groups[route.Action.CapabilityKey], route)
	}
	overrides := schedulerOperationOverrides(routes)
	definitions := []struct {
		key, name, description string
		chains, scopes         []string
	}{
		{SchedulerAuthoringCategory, "Scheduler authoring", "Read, validate, preview, and simulate published Scheduler definitions.", []string{"workflow_or_report_target_to_scheduler_job", "integration_connection_to_scheduler_http_target"}, []string{"scheduler.definition"}},
		{SchedulerOperationsCategory, "Scheduler operations", "Inspect durable Scheduler execution evidence and issue idempotent run, reschedule, retry, cancel, resolve, and requeue commands.", []string{"scheduler_job_to_durable_run", "scheduler_dead_letter_to_operator_recovery"}, []string{}},
	}
	documents := make([]modulecapability.CategoryDocument, 0, len(definitions))
	for _, definition := range definitions {
		document, err := modulecapability.CategoryFromHTTPRoutes(modulecapability.HTTPRouteCategory{
			Owner: "scheduler", Category: modulecapability.CategorySummary{Key: definition.key, Name: definition.name, Description: definition.description, AssemblyChains: definition.chains, ValidationScopes: definition.scopes},
			Routes: groups[definition.key], Operations: operations, WorkspaceScope: "authenticated_workspace", ExtensionOverrides: overrides,
			Components: map[string]map[string]json.RawMessage{
				"securitySchemes": {"BearerAuth": json.RawMessage(`{"type":"http","scheme":"bearer","bearerFormat":"JWT"}`)},
			},
		})
		if err != nil {
			return nil, fmt.Errorf("project Scheduler category %q: %w", definition.key, err)
		}
		if definition.key == SchedulerAuthoringCategory {
			document.ValidationContracts = []modulecapability.ValidationScopeContract{{
				Kind: "scheduler.definition", Description: "Validate one complete scheduled job definition, including its nested schedule.", Coverage: modulecapability.ValidationCoverageAllCandidates, CandidateCollections: []string{"scheduled_jobs"},
			}}
			value, sourceErr := schedulerBlueprintSourceCapability()
			if sourceErr != nil {
				return nil, sourceErr
			}
			payload, marshalErr := json.Marshal(value)
			if marshalErr != nil {
				return nil, marshalErr
			}
			document.Projections = append(document.Projections, modulecapability.SourceProjection{Kind: schedulerBlueprintSourceProjectionKind, Key: value.Key, Payload: payload})
		}
		documents = append(documents, document)
	}
	summary := modulecapability.ModuleSummary{
		Identity: modulecapability.ModuleIdentity{
			Key: "scheduler", SourceOwner: "scheduler", ModuleVersion: schedulersdk.ProtocolVersionV1, ValidationRevision: "scheduler-authoring-validation-v1",
			SupportedDeploymentModes: []modulecapability.DeploymentMode{modulecapability.DeploymentModeModule, modulecapability.DeploymentModeSaaS},
		},
		Name: "Scheduler", Description: "Owns clocks, recurring schedule evaluation, durable trigger claims, retry evidence, runs, and dead letters while downstream modules retain business execution semantics.",
		Scenarios: modulecapability.AdaptationScenarios{
			UseWhen:              []string{"A PRD needs work to run at a future or recurring time, manual scheduled-job execution, bounded catch-up, or durable retry and dead-letter recovery"},
			DoNotUseWhen:         []string{"The requirement is an event-driven business reaction, a multi-step human workflow, retention cleanup ownership, or notification delivery without a time schedule"},
			RequirementSignals:   []string{"cron", "daily weekly monthly schedule", "run at", "recurring job", "manual trigger", "retry run", "dead letter", "catch up"},
			ProvidedCapabilities: []string{"scheduler.business_job", "scheduler.schedule", "scheduler.job.simulate", "scheduler.job.run", "scheduler.definition.reschedule", "scheduler.run.retry", "scheduler.run.cancel", "scheduler.dead_letter.resolve", "scheduler.dead_letter.requeue", "scheduler.run_evidence"},
			RequiredModules:      []string{}, OptionalModules: []string{"integration", "report", "workflow"}, ConflictingModules: []string{},
			AssemblyChains:    []string{"workflow_or_report_target_to_scheduler_job", "integration_connection_to_scheduler_http_target", "scheduler_job_to_durable_run", "scheduler_dead_letter_to_operator_recovery"},
			ValidationScopes:  []string{"scheduler.definition"},
			SelectionExamples: []modulecapability.ScenarioExample{{Requirement: "Refresh a report snapshot every weekday at 09:00 and preserve retry evidence", Reason: "Scheduler owns recurrence, durable claims, retries, and run evidence while Report owns the refresh operation"}},
			RejectionExamples: []modulecapability.ScenarioExample{{Requirement: "Send an Inbox message immediately when an order is approved", Reason: "That is event-driven Notification dispatch; Scheduler is selected only when time controls execution"}},
		},
	}
	return modulecapability.NewStaticBinding(summary, documents, ValidateCandidate)
}

func ValidateCandidate(ctx context.Context, request modulecapability.ValidationRequest) (modulecapability.ValidationResult, error) {
	var candidate scheduledJobSource
	if err := modulecapability.DecodeKeyedAuthoringValue(request.Candidate, "key", &candidate); err != nil {
		return invalidResult("scheduler.candidate_invalid", "$.candidate.value", nil, err), nil
	}
	var err error
	switch request.Kind {
	case "scheduler.definition":
		if strings.TrimSpace(candidate.Key) == "" {
			return invalidResult("scheduler.key_required", "$.candidate.value.key", nil, fmt.Errorf("scheduled job key is required")), nil
		}
		if candidate.Key != request.Candidate.Key {
			return invalidResult("scheduler.key_mismatch", "$.candidate.value.key", map[string]string{"expected": request.Candidate.Key, "actual": candidate.Key}, fmt.Errorf("scheduled job key differs from fragment key")), nil
		}
		if !validScheduledJobStatus(candidate.Status) {
			return invalidResult("scheduler.status_invalid", "$.candidate.value.status", map[string]string{"allowed": "disabled,draft,enabled,paused", "actual": candidate.Status}, fmt.Errorf("scheduled job status is invalid")), nil
		}
		scheduleType, inferErr := candidate.Schedule.inferredType()
		if inferErr != nil {
			return invalidResult("scheduler.schedule_shape_invalid", "$.candidate.value.schedule", nil, inferErr), nil
		}
		if candidate.Schedule.Type != "" && candidate.Schedule.Type != scheduleType {
			return invalidResult("scheduler.schedule_type_derived_mismatch", "$.candidate.value.schedule.type", map[string]string{"expected": scheduleType, "actual": candidate.Schedule.Type}, fmt.Errorf("schedule type must match the compiler-derived source shape")), nil
		}
		err = schedule.ValidateDefinitionData(ctx, candidate.runtimeData(scheduleType))
	default:
		return modulecapability.ValidationResult{}, &modulecapability.Error{StatusCode: 400, Code: "module_capability.validation_scope_invalid"}
	}
	if err == nil {
		return modulecapability.ValidationResult{Diagnostics: []modulecapability.Diagnostic{}}, nil
	}
	rule := schedule.ValidationCode(err)
	if rule == "" {
		rule = "scheduler.candidate_invalid"
	}
	params := schedule.ValidationParams(err)
	field := "$.candidate.value"
	if value := strings.TrimSpace(params["field"]); value != "" {
		field += "." + scheduledJobSourceField(value)
	}
	return invalidResult(rule, field, params, err), nil
}

type scheduledJobSource struct {
	Key                string                     `json:"key"`
	Name               string                     `json:"name"`
	Status             string                     `json:"status"`
	TargetType         string                     `json:"target_type"`
	TargetKey          string                     `json:"target_key"`
	Schedule           scheduledJobScheduleSource `json:"schedule"`
	MissedWindowPolicy string                     `json:"missed_window_policy"`
	MaxCatchupWindows  int                        `json:"max_catchup_windows,omitempty"`
	MaxAttempts        int                        `json:"max_attempts"`
	TimeoutSeconds     int                        `json:"timeout_seconds"`
	I18n               map[string]json.RawMessage `json:"i18n,omitempty"`
}

type scheduledJobScheduleSource struct {
	// Type is accepted only on Plane-normalized candidates and must equal the
	// shape-derived value. It is intentionally absent from the model-facing
	// source projection and source examples.
	Type            string `json:"type,omitempty"`
	Expression      string `json:"expression,omitempty"`
	IntervalSeconds *int   `json:"interval_seconds,omitempty"`
	TimeOfDay       string `json:"time_of_day,omitempty"`
	DayOfWeek       string `json:"day_of_week,omitempty"`
	DayOfMonth      *int   `json:"day_of_month,omitempty"`
	Timezone        string `json:"timezone,omitempty"`
}

func (value scheduledJobScheduleSource) inferredType() (string, error) {
	expression := strings.TrimSpace(value.Expression) != ""
	interval := value.IntervalSeconds != nil
	timeOfDay := strings.TrimSpace(value.TimeOfDay) != ""
	dayOfWeek := strings.TrimSpace(value.DayOfWeek) != ""
	dayOfMonth := value.DayOfMonth != nil
	timezone := strings.TrimSpace(value.Timezone) != ""
	switch {
	case expression && !interval && !timeOfDay && !dayOfWeek && !dayOfMonth:
		return "cron", nil
	case interval && !expression && !timeOfDay && !dayOfWeek && !dayOfMonth && !timezone:
		return "interval", nil
	case timeOfDay && !expression && !interval && !dayOfWeek && !dayOfMonth:
		return "daily_at", nil
	case timeOfDay && dayOfWeek && !expression && !interval && !dayOfMonth:
		return "weekly_at", nil
	case timeOfDay && dayOfMonth && !expression && !interval && !dayOfWeek:
		return "monthly_at", nil
	default:
		return "", fmt.Errorf("schedule must identify exactly one cron, interval, daily, weekly, or monthly source shape")
	}
}

func (value scheduledJobSource) runtimeData(scheduleType string) map[string]any {
	targetKey := strings.TrimSpace(value.TargetKey)
	if value.TargetType == "workflow" {
		targetKey = "scheduled:" + targetKey
	}
	result := map[string]any{
		"key": value.Key, "name": value.Name, "status": value.Status,
		"target_type": value.TargetType, "target_key": targetKey,
		"schedule_type":        scheduleType,
		"missed_window_policy": value.MissedWindowPolicy,
		"max_attempts":         value.MaxAttempts, "timeout_seconds": value.TimeoutSeconds,
	}
	if value.MaxCatchupWindows != 0 {
		result["max_catchup_windows"] = value.MaxCatchupWindows
	}
	if value.Schedule.Expression != "" {
		result["schedule_expression"] = value.Schedule.Expression
	}
	if value.Schedule.IntervalSeconds != nil {
		result["interval_seconds"] = *value.Schedule.IntervalSeconds
	}
	if value.Schedule.TimeOfDay != "" {
		result["time_of_day"] = value.Schedule.TimeOfDay
	}
	if value.Schedule.DayOfWeek != "" {
		result["day_of_week"] = value.Schedule.DayOfWeek
	}
	if value.Schedule.DayOfMonth != nil {
		result["day_of_month"] = *value.Schedule.DayOfMonth
	}
	if value.Schedule.Timezone != "" {
		result["timezone"] = value.Schedule.Timezone
	}
	return result
}

func validScheduledJobStatus(value string) bool {
	switch strings.TrimSpace(value) {
	case "draft", "enabled", "paused", "disabled":
		return true
	default:
		return false
	}
}

func scheduledJobSourceField(value string) string {
	switch value {
	case "schedule_type":
		return "schedule.type"
	case "schedule_expression":
		return "schedule.expression"
	case "interval_seconds", "time_of_day", "day_of_week", "day_of_month", "timezone":
		return "schedule." + value
	default:
		return value
	}
}

func invalidResult(rule, field string, params map[string]string, err error) modulecapability.ValidationResult {
	message := rule
	if err != nil && strings.TrimSpace(err.Error()) != "" {
		message = err.Error()
	}
	return modulecapability.ValidationResult{Diagnostics: []modulecapability.Diagnostic{{Owner: "scheduler", RuleKey: rule, Severity: modulecapability.SeverityError, FieldPath: field, Message: message, Params: params}}}
}

func schedulerHTTPContract() ([]modulehttp.Route, map[string]map[string]any, error) {
	actions, err := schedulersdk.SchedulerAuthorizationActions()
	if err != nil {
		return nil, nil, err
	}
	operations, err := schedulersdk.SchedulerHTTPOpenAPIOperations()
	if err != nil {
		return nil, nil, err
	}
	routes := make([]modulehttp.Route, 0, len(actions))
	for _, action := range actions {
		projected, err := modulehttp.RouteFromAction(action)
		if err != nil {
			return nil, nil, fmt.Errorf("project Scheduler Action %q: %w", action.Key, err)
		}
		routes = append(routes, projected)
	}
	return routes, operations, nil
}

func schedulerOperationOverrides(routes []modulehttp.Route) map[string]modulecapability.OperationExtension {
	result := map[string]modulecapability.OperationExtension{}
	for _, route := range routes {
		pattern := route.Pattern()
		if route.Action.CapabilityKey == SchedulerOperationsCategory && route.Action.EffectClass == actioncontract.EffectWrite {
			result[pattern] = modulecapability.OperationExtension{
				Owner: "scheduler", Authorization: modulecapability.Authorization{Strategy: actioncontract.AuthorizationAuthenticated, Permission: route.Action.Key, WorkspaceScope: "authenticated_workspace"},
				Effect: modulecapability.EffectWrite, Idempotency: modulecapability.Idempotency{Mode: "caller_key_required", KeySource: "Idempotency-Key"},
			}
		}
	}
	return result
}
