package capability

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/domainry/domainry-foundation/modulecapability"
	"github.com/domainry/domainry-foundation/modulecapability/contracttest"
	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	capabilitycontract "github.com/domainry/domainry-scheduler-sdk/authoring/contract"
)

func TestSchedulerCapabilityPublishesCompleteSDKOperations(t *testing.T) {
	binding, err := NewBinding()
	if err != nil {
		t.Fatal(err)
	}
	want, err := schedulersdk.SchedulerHTTPOpenAPIOperations()
	if err != nil {
		t.Fatal(err)
	}
	summary, err := binding.CapabilitySummary(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, category := range summary.Categories {
		document, err := binding.CapabilityCategory(t.Context(), category.Key)
		if err != nil {
			t.Fatal(err)
		}
		for path, methods := range document.OpenAPI.Paths {
			for method, raw := range methods {
				var operation map[string]any
				if err := json.Unmarshal(raw, &operation); err != nil {
					t.Fatal(err)
				}
				delete(operation, modulecapability.OperationExtensionKey)
				assertCapabilityCanonicalEqual(t, operation, want[strings.ToUpper(method)+" "+path])
				count++
			}
		}
	}
	if count != len(want) {
		t.Fatalf("capability operations=%d SDK operations=%d", count, len(want))
	}
}

func TestSchedulerCapabilityTracksExternalRoutesAuthoringAndValidation(t *testing.T) {
	binding, err := NewBinding()
	if err != nil {
		t.Fatal(err)
	}
	contracttest.VerifyBinding(t, binding)
	summary, err := binding.CapabilitySummary(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	operations, projections := 0, 0
	for _, category := range summary.Categories {
		operations += category.OperationCount
		projections += category.ProjectionCount
	}
	if operations != 13 || projections != 1 {
		t.Fatalf("Scheduler operations=%d projections=%d", operations, projections)
	}
	request := modulecapability.ValidationRequest{
		ContractVersion: modulecapability.ValidationContractVersion, ModuleKey: "scheduler", CategoryKey: SchedulerAuthoringCategory,
		ContractSHA256: summary.Identity.ContractSHA256, Kind: "scheduler.definition", Candidate: modulecapability.AuthoringFragment{Collection: "scheduled_jobs", Key: "daily", Value: json.RawMessage(`{"key":"daily","name":"Daily","status":"enabled","target_type":"workflow","target_key":"daily","schedule":{"type":"interval","interval_seconds":0},"missed_window_policy":"skip","max_attempts":3,"timeout_seconds":300}`)},
	}
	result, err := binding.ValidateCapabilityCandidate(t.Context(), request)
	if err != nil || len(result.Diagnostics) != 1 || result.Diagnostics[0].RuleKey != "backend.scheduler.interval_invalid" || result.Diagnostics[0].FieldPath != "$.candidate.value.schedule.interval_seconds" {
		t.Fatalf("Scheduler diagnostics=%+v err=%v", result.Diagnostics, err)
	}
	verifyCapabilityRemoteParityWithoutListener(t, binding, request)
}

type capabilityHandlerTransport struct{ handler http.Handler }

func (t capabilityHandlerTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response := httptest.NewRecorder()
	t.handler.ServeHTTP(response, request)
	return response.Result(), nil
}

func verifyCapabilityRemoteParityWithoutListener(t *testing.T, direct modulecapability.Binding, validationRequest modulecapability.ValidationRequest) {
	t.Helper()
	directSummary, err := direct.CapabilitySummary(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	handler, err := modulecapability.NewHTTPHandler(direct, func(request *http.Request) error {
		if request.Header.Get("X-Domainry-Capability-Test") != directSummary.Identity.Key {
			return &modulecapability.Error{StatusCode: http.StatusUnauthorized, Code: "module_capability.service_authentication_required"}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: capabilityHandlerTransport{handler: handler}}
	open := func(hash string) (modulecapability.Binding, error) {
		return modulecapability.OpenRemote(t.Context(), modulecapability.RemoteConfig{
			BaseURL: "http://scheduler-capability.test", Client: client, ExpectedModuleKey: directSummary.Identity.Key, ExpectedContractSHA256: hash,
			Authorize: func(request *http.Request) error {
				request.Header.Set("X-Domainry-Capability-Test", directSummary.Identity.Key)
				return nil
			},
		})
	}
	remote, err := open(directSummary.Identity.ContractSHA256)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := open(strings.Repeat("0", 64)); err == nil || !strings.Contains(err.Error(), "module_capability.contract_mismatch") {
		t.Fatalf("Remote binding accepted a stale capability digest: %v", err)
	}
	remoteSummary, err := remote.CapabilitySummary(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	assertCapabilityCanonicalEqual(t, directSummary, remoteSummary)
	for _, category := range directSummary.Categories {
		directDocument, directErr := direct.CapabilityCategory(t.Context(), category.Key)
		remoteDocument, remoteErr := remote.CapabilityCategory(t.Context(), category.Key)
		if directErr != nil || remoteErr != nil {
			t.Fatalf("category %q direct=%v remote=%v", category.Key, directErr, remoteErr)
		}
		assertCapabilityCanonicalEqual(t, directDocument, remoteDocument)
	}
	directResult, directErr := direct.ValidateCapabilityCandidate(t.Context(), validationRequest)
	remoteResult, remoteErr := remote.ValidateCapabilityCandidate(t.Context(), validationRequest)
	if fmt.Sprint(directErr) != fmt.Sprint(remoteErr) {
		t.Fatalf("validation errors differ: direct=%v remote=%v", directErr, remoteErr)
	}
	assertCapabilityCanonicalEqual(t, directResult, remoteResult)
}

func assertCapabilityCanonicalEqual(t *testing.T, left, right any) {
	t.Helper()
	leftBytes, err := modulecapability.CanonicalJSON(left)
	if err != nil {
		t.Fatal(err)
	}
	rightBytes, err := modulecapability.CanonicalJSON(right)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(leftBytes, rightBytes) {
		t.Fatalf("canonical values differ\nleft:  %s\nright: %s", leftBytes, rightBytes)
	}
}

func TestSchedulerBlueprintSourceProjectionMatchesOwnerValidator(t *testing.T) {
	binding, err := NewBinding()
	if err != nil {
		t.Fatal(err)
	}
	document, err := binding.CapabilityCategory(t.Context(), SchedulerAuthoringCategory)
	if err != nil {
		t.Fatal(err)
	}
	if len(document.Projections) != 1 {
		t.Fatalf("Scheduler source projections=%+v", document.Projections)
	}
	projection := document.Projections[0]
	if projection.Kind != schedulerBlueprintSourceProjectionKind || projection.Key != "scheduler.business_job" {
		t.Fatalf("Scheduler source projection=%+v", projection)
	}
	var capability capabilitycontract.CapabilityAuthoringDefinition
	if err := json.Unmarshal(projection.Payload, &capability); err != nil {
		t.Fatal(err)
	}
	if capability.Lifecycle != "domain_blueprint_source_fragment" || capability.InputSchema == nil || capability.InputSchema.AdditionalProperties == nil || *capability.InputSchema.AdditionalProperties {
		t.Fatalf("Scheduler Blueprint source capability=%+v", capability)
	}
	properties := capability.InputSchema.Properties
	for _, forbidden := range []string{"trigger_type", "schedule_type", "schedule_expression", "connection_key", "run_as_role"} {
		if _, exists := properties[forbidden]; exists {
			t.Fatalf("flattened management field %q leaked into Blueprint source schema", forbidden)
		}
	}
	schedule, exists := properties["schedule"]
	if !exists || len(schedule.OneOf) != 5 {
		t.Fatalf("nested schedule schema=%+v", schedule)
	}

	summary, err := binding.CapabilitySummary(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, example := range capability.Examples {
		fragment, fragmentErr := modulecapability.NewAuthoringFragment("scheduled_jobs", example.Value["key"].(string), example.Value)
		if fragmentErr != nil {
			t.Fatal(fragmentErr)
		}
		result, validateErr := binding.ValidateCapabilityCandidate(t.Context(), modulecapability.ValidationRequest{
			ContractVersion: modulecapability.ValidationContractVersion, ModuleKey: "scheduler", CategoryKey: SchedulerAuthoringCategory,
			ContractSHA256: summary.Identity.ContractSHA256, Kind: "scheduler.definition", Candidate: fragment,
		})
		if validateErr != nil || len(result.Diagnostics) != 0 {
			t.Fatalf("source example %q is not accepted by owner validator: diagnostics=%+v err=%v", example.Name, result.Diagnostics, validateErr)
		}
	}
}

func TestSchedulerBlueprintSourceSchemaAndStrictDecoderStayIsomorphic(t *testing.T) {
	capability, err := schedulerBlueprintSourceCapability()
	if err != nil {
		t.Fatal(err)
	}
	wantTop := jsonFieldNames(reflect.TypeOf(scheduledJobSource{}))
	gotTop := schemaPropertyNames(capability.InputSchema.Properties)
	if !reflect.DeepEqual(gotTop, wantTop) {
		t.Fatalf("top-level schema fields=%v strict decoder fields=%v", gotTop, wantTop)
	}
	scheduleSchema := capability.InputSchema.Properties["schedule"]
	wantSchedule := jsonFieldNames(reflect.TypeOf(scheduledJobScheduleSource{}))
	wantSchedule = removeString(wantSchedule, "type")
	for _, variant := range scheduleSchema.OneOf {
		if _, exists := variant.Properties["type"]; exists {
			t.Fatal("compiler-derived schedule.type leaked into the model-facing source projection")
		}
		for field := range variant.Properties {
			if !contains(wantSchedule, field) {
				t.Fatalf("schedule schema field %q is not decoded by scheduledJobScheduleSource", field)
			}
		}
	}
	for _, field := range wantSchedule {
		found := false
		for _, variant := range scheduleSchema.OneOf {
			_, found = variant.Properties[field]
			if found {
				break
			}
		}
		if !found {
			t.Fatalf("strict schedule field %q is absent from every source schema variant", field)
		}
	}
}

func TestSchedulerBlueprintSourceRejectsRuntimeAndConflictingDerivedFields(t *testing.T) {
	for name, value := range map[string]json.RawMessage{
		"runtime trigger":                   json.RawMessage(`{"status":"enabled","trigger_type":"scheduled","target_type":"workflow","target_key":"daily","schedule":{"interval_seconds":60},"missed_window_policy":"skip","max_attempts":1,"timeout_seconds":300}`),
		"conflicting derived schedule type": json.RawMessage(`{"status":"enabled","target_type":"workflow","target_key":"daily","schedule":{"type":"cron","interval_seconds":60},"missed_window_policy":"skip","max_attempts":1,"timeout_seconds":300}`),
	} {
		t.Run(name, func(t *testing.T) {
			result, err := ValidateCandidate(t.Context(), modulecapability.ValidationRequest{
				Kind: "scheduler.definition", Candidate: modulecapability.AuthoringFragment{Collection: "scheduled_jobs", Key: "daily", Value: value},
			})
			if err != nil || len(result.Diagnostics) != 1 {
				t.Fatalf("diagnostics=%+v err=%v", result.Diagnostics, err)
			}
			want := "scheduler.candidate_invalid"
			if name == "conflicting derived schedule type" {
				want = "scheduler.schedule_type_derived_mismatch"
			}
			if result.Diagnostics[0].RuleKey != want {
				t.Fatalf("diagnostic=%+v want rule=%s", result.Diagnostics[0], want)
			}
		})
	}
}

func TestSchedulerBlueprintSourceCapabilityGolden(t *testing.T) {
	capability, err := schedulerBlueprintSourceCapability()
	if err != nil {
		t.Fatal(err)
	}
	actual, err := json.MarshalIndent(capability, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	actual = append(actual, '\n')
	expected, err := os.ReadFile("testdata/scheduler-blueprint-source-capability.golden.json")
	if err != nil {
		t.Fatalf("read golden: %v\n%s", err, actual)
	}
	if !bytes.Equal(actual, expected) {
		t.Fatalf("Scheduler Blueprint source capability changed; update the reviewed golden deliberately\nactual:\n%s", actual)
	}
}

func jsonFieldNames(value reflect.Type) []string {
	result := make([]string, 0, value.NumField())
	for index := 0; index < value.NumField(); index++ {
		name := value.Field(index).Tag.Get("json")
		for offset, char := range name {
			if char == ',' {
				name = name[:offset]
				break
			}
		}
		if name != "" && name != "-" {
			result = append(result, name)
		}
	}
	sort.Strings(result)
	return result
}

func schemaPropertyNames(values map[string]capabilitycontract.CapabilityAuthoringSchema) []string {
	result := make([]string, 0, len(values))
	for key := range values {
		result = append(result, key)
	}
	sort.Strings(result)
	return result
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func removeString(values []string, target string) []string {
	result := values[:0]
	for _, value := range values {
		if value != target {
			result = append(result, value)
		}
	}
	return result
}
