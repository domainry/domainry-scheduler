# Should a delay use Scheduler or remain inside Workflow?

## Problems solved

- Prevents one delayed process continuation from being modeled as recurring Scheduler work, and prevents recurrence from being hidden inside Workflow state.

## Business scenarios

- Resuming one approval process at its deadline through a Workflow wait.
- Starting a new overdue-approval scan every night through Scheduler recurrence.

## Use when

Choose Scheduler when independent work starts from a calendar/interval. Keep a one-time continuation inside an already running Workflow.

## Do not use when

Do not create one Scheduler definition per waiting business record or hide a recurring job inside Workflow timers.

## How to use

Ask whether there is an existing process instance. If yes, persist its wait/continuation in Workflow. If no and time itself starts reusable work, define a Scheduler target.

## Adaptation cookbook

| Business requirement | Adapt with | Concrete implementation | Wrong adaptation |
| --- | --- | --- | --- |
| One approval instance must resume at its deadline | Workflow wait/timer | Persist deadline and correlation inside the Workflow instance; resume exactly that instance once, including after restart | Creating a standalone recurring Scheduler definition for each approval |
| Every night the system scans all overdue approvals | Scheduler recurrence targeting a bounded scan Operation | Scheduler owns nightly timezone/catch-up/run evidence; the target queries authorized overdue work and acts idempotently | Keeping one Workflow instance alive forever to implement a global nightly loop |
| Workflow retries a step shortly after a transient owner failure | Workflow step retry when it belongs to process execution | Preserve step/process context and target idempotency in Workflow | Creating a reusable Scheduler schedule for an internal step retry |
| A monthly Workflow must start as a new process instance | Scheduler targeting Workflow start | Give each calendar occurrence a stable run key and typed period input; Workflow owns the newly started process | Modeling recurrence as a wait at the end of one never-ending Workflow instance |

## Example

“Resume this approval at its deadline” is Workflow. “Every night scan overdue approvals” is Scheduler invoking a published scan Operation.

## Permissions and scope

Workflow task authority and Scheduler service authority are separate. Neither inherits the original user’s full session.

## Boundaries

Workflow owns process state; Scheduler owns independent clocks and run evidence.
