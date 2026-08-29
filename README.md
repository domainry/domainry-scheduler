# Domainry Scheduler

`domainry-scheduler` owns recurrence planning, durable trigger orchestration and downstream dispatch evidence. It does not own Workflow, Report, Connector or other business execution semantics.

## Deployment topologies

- Module: inject `module.NewFactory(module.OptionsFromEnvironment())` through `runtimehost.Options.SchedulerFactory`. Runtime lends published definitions, durable run state and downstream dispatch ports.
- SaaS: inject `remote.NewHTTPFactory(httptransport.ConfigFromEnvironment())`. Runtime publishes a revisioned definition snapshot to the Scheduler SaaS endpoint.

SaaS environment variables:

- `SCHEDULER_SAAS_ENDPOINT`
- `SCHEDULER_SAAS_TOKEN`

The `server` package exposes the matching authenticated HTTP protocol over an implementation of `server.Service`.

## Targets

- `runtime_operation`: Scheduler emits a trigger to the Runtime-owned downstream dispatcher.
- `http`: definitions reference `connection_key` and `operation`; URLs and credentials remain deployment-owned.
  - `runtime_callback` uses Runtime Integration/Connector execution and supports private endpoints.
  - `direct` uses Scheduler's HTTP executor, with idempotency/window headers, bounded responses, HMAC signing and retry classification.

Definitions are configuration. A successful downstream call must return a durable receipt before a run is accepted.
