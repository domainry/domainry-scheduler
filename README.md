# Domainry Scheduler

`domainry-scheduler` owns recurrence planning, durable trigger orchestration and downstream dispatch evidence. It does not own Workflow, Report, Connector or other business execution semantics.

## Deployment topologies

- Module: inject `module.NewFactory(module.OptionsFromEnvironment())` through `runtimehost.Options.SchedulerFactory`. Runtime lends published definitions, durable run state and downstream dispatch ports.
- SaaS: inject `remote.NewHTTPFactory(httptransport.ConfigFromEnvironment())`. Runtime publishes a revisioned definition snapshot to the Scheduler SaaS endpoint.

SaaS environment variables:

- `SCHEDULER_SAAS_ENDPOINT`
- `SCHEDULER_SAAS_TOKEN`

For a single-Runtime Scheduler SaaS process, the callback gateway can be
configured with:

- `SCHEDULER_RUNTIME_ENDPOINT`
- `SCHEDULER_RUNTIME_SERVICE_CREDENTIAL`

The service composition is intentionally explicit:

```go
gateway, err := dispatchgateway.NewRemote(dispatchgateway.RemoteConfigFromEnvironment())
service, err := server.NewDatabaseService(server.DatabaseServiceOptions{
    Database: db, Driver: driver, Schema: schema, WorkerID: workerID,
    Worker: schedulersdk.WorkerConfig{Enabled: true},
    Downstreams: server.RemoteDownstreams(gateway),
})
handler := server.New(server.Options{BearerToken: controlPlaneToken, Service: service})
```

Multi-tenant services use `server.RemoteDownstreamsByApplication` to resolve a
different callback endpoint and credential for each Runtime. Scheduler tables,
leases, run evidence and the standalone `_schema_migrations` ledger stay in the
Scheduler database. Module mode uses the Runtime pool and shared migration
ledger, but the same tables and repository remain Scheduler-owned.

The `server` package exposes the matching authenticated HTTP protocol over an implementation of `server.Service`.

## Targets

- `runtime_operation`: Scheduler emits a trigger to the Runtime-owned downstream dispatcher.
- `http`: definitions reference `connection_key` and `operation`; URLs and credentials remain deployment-owned.
  - `runtime_callback` uses Runtime Integration/Connector execution and supports private endpoints.
  - `direct` uses Scheduler's HTTP executor, with idempotency/window headers, bounded responses, HMAC signing and retry classification.

Definitions are configuration. A successful downstream call must return a durable receipt before a run is accepted.

## Repository layout

```text
admin/                    Scheduler target-editor npm package
internal/executor/http/   private direct-HTTP dispatch adapter
module/                   public in-process Module factory and binding
remote/                   public Scheduler SaaS client binding
server/                   public Scheduler SaaS HTTP server protocol
```

Only `module`, `remote`, and `server` are public Go packages. Implementation
adapters stay below `internal` so applications cannot couple to them directly.
The deterministic recurrence functions live in
`domainry-scheduler-sdk/schedule` beside the shared `Schedule` contract and are
used by Module, SaaS, and Runtime compatibility paths.
