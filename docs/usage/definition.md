# How should a recurring or future schedule be defined?

## Problems solved

- Turns business calendar or interval intent into an explicit schedule with timezone, catch-up, simulation, and target contracts.

## Business scenarios

- Running a weekday Report refresh at 09:00 in Asia/Shanghai.
- Starting a monthly close Workflow or a bounded interval job with a declared missed-window policy.
- Running a governed Business Action under a managed service Role and skipping or rolling occurrences on a versioned non-working date.
- Calling a published HTTP target through a governed Integration connection rather than storing a URL or credential in the schedule.
- Distinguishing a one-off Workflow continuation from an independently recurring definition.

## Use when

Use a schedule when time independently starts a published target at one future time or a recurrence.

## Do not use when

Do not schedule internal event reactions. Do not put target business logic inside the schedule definition.

## How to use

Define timezone, recurrence/instant, missed-window policy, bounded catch-up, target operation, target input, and idempotency. A `business_action` target also requires `target_object` and `run_as_role`; the Role must be a system-managed service Role with the exact Action permission. Simulate future triggers before enabling.

For a calendar schedule, `business_calendar_key` selects the Runtime-owned immutable calendar snapshot and `non_working_day_policy` is exactly `skip` or `roll_forward`. The schedule timezone must match the calendar timezone. Fixed intervals cannot use a business calendar because they express elapsed time rather than local-date occurrences.

## Adaptation cookbook

| Business requirement | Adapt with | Concrete implementation | Wrong adaptation |
| --- | --- | --- | --- |
| Refresh a Report every weekday at 09:00 Asia/Shanghai | Calendar schedule with explicit timezone and Report target | Publish weekday/calendar expression, timezone, target definition/version/parameters, concurrency rule, and missed-window policy; simulate upcoming runs before enablement | Encoding local 09:00 as server UTC or hiding the schedule inside Report code |
| Start monthly close Workflow on the first business day | Calendar schedule targeting a Workflow start contract | Reference the published business calendar, choose `skip` or `roll_forward`, create one run identity per period, and pass the close period as typed input | Modeling the monthly process as a Workflow loop that sleeps until next month |
| Poll a provider every 15 minutes | Fixed interval schedule targeting an Integration synchronization Operation | Declare interval anchor, overlap policy, timeout, and idempotent sync cursor | Using a calendar expression whose drift/overlap semantics are undefined |
| Expire overdue orders every morning | `business_action` target | Declare the order Object, exact Action key, JSON object payload, and a least-privilege system-managed service Role; Runtime executes through the Action Application Service | Calling the project Handler directly or silently running as installation system |
| Invoke a governed external synchronization target | `http` target plus `connection_key` | Select a published target key and Integration-owned connection, keep the payload bounded, and preserve Scheduler window identity across delivery retries; if Deck lacks this shape, add the typed Scheduler adapter before authoring it | Putting an arbitrary URL, bearer token, or generic callback in the schedule, or dropping the requirement because an adapter is missing |
| A missed nightly maintenance window should not run during peak hours | Explicit skip/catch-up policy | Configure skip or bounded catch-up window and make the decision visible in run history | Automatically replaying every missed occurrence without considering business impact |

## Example

For an hourly Report snapshot, publish a definition with `schedule_type: "cron"`, `schedule_expression: "0 2 * * *"`, explicit timezone, `target_type: "report_snapshot_refresh"`, the published Report key, bounded catch-up, retry, and timeout. Preview the next occurrences before enablement. Scheduler claims one stable identity per window; Report owns refresh and snapshot replacement.

For Monday workflow startup, use `target_type: "workflow"` and a `scheduled:<key>` target; the Workflow owns all later tasks and waits. For overdue-order automation, use `business_action` with exact Action/Object, JSON-object payload, and a `service` + `system_managed` Role holding only that Action permission. A calendar schedule may choose `skip` or `roll_forward` against the immutable Runtime calendar snapshot. `http` requires a governed `connection_key`. These are published source-contract capabilities. When Deck has not yet lowered one of the shapes, extend `scheduled_jobs`, semantic validation, manifest lowering, generated contracts, and target-specific acceptance tests instead of declaring the business scenario unsupported.

## Permissions and scope

The scheduled service Role receives only the target operation and required data scope. Manual run permission does not imply definition edit or retry authority.

## Boundaries

Scheduler decides when and records attempts. Report, Workflow, Integration, or Runtime Action decides what the target means. Scheduler never calls a project Handler directly and never substitutes the installation system principal for a missing service Role.
