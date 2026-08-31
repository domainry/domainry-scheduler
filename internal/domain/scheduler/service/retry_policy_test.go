package service

import (
	"testing"
	"time"

	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
)

func TestNextRetryUsesDefaultAndConfiguredInitialDelay(t *testing.T) {
	now := time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC)
	if got := NextRetry(schedulersdk.Policy{}, 1, now); !got.Equal(now.Add(30 * time.Second)) {
		t.Fatalf("default retry=%s", got)
	}
	if got := NextRetry(schedulersdk.Policy{RetryInitial: 5 * time.Second}, 1, now); !got.Equal(now.Add(5 * time.Second)) {
		t.Fatalf("configured retry=%s", got)
	}
}
