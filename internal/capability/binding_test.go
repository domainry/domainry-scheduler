package capability

import (
	"encoding/json"
	"testing"

	"github.com/domainry/domainry-foundation/modulecapability"
	"github.com/domainry/domainry-foundation/modulecapability/contracttest"
)

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
	if operations != 13 || projections != 7 {
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
	contracttest.VerifyModuleRemoteParity(t, binding, contracttest.ValidationCase{Name: "invalid interval", Request: request})
}
