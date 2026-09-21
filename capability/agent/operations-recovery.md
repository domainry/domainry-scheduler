# How should Scheduler runs, retries, cancellation, and dead letters be operated?

## Problems solved

- Gives operators idempotent commands and durable evidence for exceptional scheduled execution without editing job definitions or repeating downstream effects blindly.

## Business scenarios

- An operator retries a failed Report refresh after the dependency recovers while preserving the original run lineage.
- A poisoned recurring run is cancelled or moved to a dead letter, then explicitly resolved or requeued after diagnosis.
- An operator manually triggers one published Definition for verification without changing its target or idempotency rules.
- A downstream timeout may have committed its effect, so recovery reconciles evidence before deciding whether retry is safe.

## Use when

Use Scheduler operations when a durable scheduled run needs inspection, manual run, reschedule, retry, cancellation, or dead-letter resolution.

## Do not use when

Do not use operator commands as normal recurrence, retry an uncertain downstream effect without reconciliation, or mutate Scheduler tables directly.

## How to use

Inspect Scheduler-owned run/dead-letter state, provide an idempotency key and operation reason, require confirmation for destructive commands, and preserve downstream execution receipts and lineage.

## Adaptation cookbook

| Operational requirement | Adapt with | Concrete implementation | Wrong adaptation |
| --- | --- | --- | --- |
| Report refresh failed on a transient dependency | Retry run command | Verify the failure is retryable, reuse Scheduler lineage/idempotency, and let Report reconcile its own receipt | Creating a new definition or directly rerunning Report with no relation to the failed run |
| Active run must stop before further target dispatch | Cancel run command | Require reason/confirmation, fence future claims, and record terminal cancellation while acknowledging already committed effects | Deleting the run row or claiming cancellation rolled back an external effect |
| Poisoned run needs manual disposition | Dead-letter resolve or requeue | Diagnose owner evidence; resolve with note when no retry is needed, or requeue once with preserved lineage after remediation | Infinite automatic requeue or editing dead-letter status in the database |
| Business wants a different future occurrence | Reschedule definition command | Change only the next scheduled time under owner permission/idempotency; retain definition and audit evidence | Rewriting cursor/run history |
| Operator verifies an enabled Definition now | Manual run command | Create one run through the same published target, service authority, and idempotency lineage, with operator reason | Calling the downstream owner directly or mutating next-run state |
| Target timed out after accepting work | Reconcile before retry | Inspect Scheduler dispatch receipt and downstream evidence; retry only when the owner proves no effect or the same identity is safe | Assuming timeout means failure and issuing a fresh business identity |
| Scheduler was offline for three expected windows | Missed-window policy, not run retry | Apply skip or bounded catch-up to occurrences that never started and record the decision | Treating uncreated windows as failed runs or replaying them without the configured bound |

## Example

A manual run reuses the published Definition's exact target and authority. For a transient failure that proves no effect, retry preserves the original run lineage and business identity. If dispatch timed out after the target may have accepted work, inspect or reconcile the downstream receipt first; a recovered terminal result closes the original run without another effect. After remediation, a dead letter is either resolved with an operator note or requeued once with lineage intact. Missed windows are handled separately by skip/bounded catch-up because no run had started.

## Permissions and scope

Read state, manual run, reschedule, retry, cancel, resolve, and requeue are distinct authorities. Cancel/resolve/requeue require explicit operational reason, and destructive operations require confirmation.

## Boundaries

Scheduler owns run/dead-letter lifecycle and clock evidence. Downstream owners decide whether their effect committed and how an uncertain result is reconciled.
