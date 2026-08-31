// Package service contains Scheduler domain policies that are independent of
// deployment topology and persistence.
package service

import (
	"time"

	"github.com/domainry/domainry-foundation/worker"
	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
)

// NextRetry applies the Scheduler-owned exponential retry policy to a failed
// run attempt. The SDK carries the policy contract; this module owns behavior.
func NextRetry(policy schedulersdk.Policy, attempt int, now time.Time) time.Time {
	baseDelay := policy.RetryInitial
	if baseDelay <= 0 {
		baseDelay = 30 * time.Second
	}
	retry := worker.RetryPolicy{BaseDelay: baseDelay, MaxDelay: policy.RetryMax, Backoff: worker.BackoffExponential}
	return now.Add(retry.Delay(attempt, nil))
}
