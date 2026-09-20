# How should a recurring or future schedule be defined?

## Problems solved

- Turns business calendar or interval intent into an explicit schedule with timezone, catch-up, simulation, and target contracts.

## Business scenarios

- Running a weekday Report refresh at 09:00 in Asia/Shanghai.
- Starting a monthly close Workflow or a bounded interval job with a declared missed-window policy.

## Use when

Use a schedule when time independently starts a published target at one future time or a recurrence.

## Do not use when

Do not schedule internal event reactions. Do not put target business logic inside the schedule definition.

## How to use

Define timezone, recurrence/instant, missed-window policy, bounded catch-up, target operation, target input, idempotency, and service Role. Simulate future triggers before enabling.

## Adaptation cookbook

| Business requirement | Adapt with | Concrete implementation | Wrong adaptation |
| --- | --- | --- | --- |
| Refresh a Report every weekday at 09:00 Asia/Shanghai | Calendar schedule with explicit timezone and Report target | Publish weekday/calendar expression, timezone, target definition/version/parameters, concurrency rule, and missed-window policy; simulate upcoming runs before enablement | Encoding local 09:00 as server UTC or hiding the schedule inside Report code |
| Start monthly close Workflow on the first business day | Calendar schedule targeting a Workflow start contract | Resolve business calendar deliberately, create one run identity per period, and pass the close period as typed input | Modeling the monthly process as a Workflow loop that sleeps until next month |
| Poll a provider every 15 minutes | Fixed interval schedule targeting an Integration synchronization Operation | Declare interval anchor, overlap policy, timeout, and idempotent sync cursor | Using a calendar expression whose drift/overlap semantics are undefined |
| A missed nightly maintenance window should not run during peak hours | Explicit skip/catch-up policy | Configure skip or bounded catch-up window and make the decision visible in run evidence | Automatically replaying every missed occurrence without considering business impact |

## Example

Refresh a Report snapshot at 09:00 every weekday in Asia/Shanghai. Scheduler claims each window once; Report validates and performs refresh.

## Permissions and scope

The scheduled service Role receives only the target operation and required data scope. Manual run permission does not imply definition edit or retry authority.

## Boundaries

Scheduler decides when and records attempts. Report/Workflow/Integration decides what the target means.
