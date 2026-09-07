# Domainry Scheduler

`domainry-scheduler` owns recurrence planning, durable trigger orchestration and downstream dispatch evidence. It does not own Workflow, Report, Connector or other business execution semantics.

## Deployment topologies

- Module: inject `module.NewFactory(module.OptionsFromEnvironment())` through `runtimehost.Options.SchedulerFactory`. Runtime lends its ORM database/dialect ports, shared migration ledger, published definitions and downstream dispatch ports; Scheduler owns the durable schema and repositories.
- SaaS client: inject `remote.NewHTTPFactory(httptransport.ConfigFromEnvironment())`. Runtime publishes a revisioned definition snapshot to the Scheduler SaaS endpoint.
- SaaS server: run `go run ./cmd/scheduler-server`. The executable owns its database pool, Scheduler worker lifecycle and authenticated HTTP transport.

The standalone process serves only the private
`/v1/applications/{runtime}/...` Runtime-to-Scheduler protocol. Each bearer
credential is configured for exactly one Runtime, and the path Runtime must
match that authenticated identity. `X-Domainry-Runtime-ID` is not trusted for
tenant selection. The source-owned 13-route `/scheduler/...` Action adapter is
mounted only in Module mode, where Runtime supplies its principal, Permission,
high-risk operation guard and audit policy. SaaS does not mount that public
surface until an equivalent trusted gateway principal contract exists.

SaaS environment variables:

- `SCHEDULER_SAAS_ENDPOINT`
- `SCHEDULER_SAAS_TOKEN`

The standalone server uses:

- `SCHEDULER_HTTP_ADDRESS` (default `:8080`)
- `SCHEDULER_DATABASE_DRIVER` (`sqlite`, `postgres`, or `mysql`)
- `SCHEDULER_DATABASE_DSN`
- `SCHEDULER_DATABASE_SCHEMA`
- `SCHEDULER_WORKER_ID`
- `SCHEDULER_RUNTIME_ID` (the sole Runtime identity bound to this process token)
- `SCHEDULER_WORKER_BATCH_SIZE` (default `100`)

For a single-Runtime Scheduler SaaS process, the callback gateway can be
configured with:

- `SCHEDULER_RUNTIME_ENDPOINT`
- `SCHEDULER_RUNTIME_SIGNING_SECRET` (same secret configured as Runtime `INTEGRATION_SECRET_KEY`)

At process start, the standalone server opens the configured
`SCHEDULER_RUNTIME_ID`, hydrates its last durable definition snapshot and
resumes its clock worker before serving traffic. A tenant with no prior
snapshot remains inactive until its first authenticated reconcile. Snapshot
revision, content hash and Scheduler-issued publisher generation are persisted
as one compare-and-swap publication cursor. A newly opened Runtime binding
begins a fresh authenticated publisher session and may publish revision 1;
older sessions remain fenced after either process restarts. Scheduler stores
only the session nonce SHA-256 digest, never the bearer nonce itself.

The standalone service composition is internal to the executable:

```sh
go run ./cmd/scheduler-server
```

The SaaS assembly can resolve a different callback endpoint and credential for
each Runtime. Scheduler tables,
leases, run evidence and the standalone `_schema_migrations` ledger stay in the
Scheduler database. Module mode uses the Runtime pool and shared migration
ledger, but the same tables and repository remain Scheduler-owned.

The HTTP protocol is an internal transport. Consumers use the published SDK
transport rather than importing the server implementation.

## Targets

- `runtime_operation`: Scheduler emits a trigger to the Runtime-owned downstream dispatcher.
- `http`: definitions reference `connection_key` and `operation`; URLs and credentials remain deployment-owned.
  - `runtime_callback` uses Runtime Integration/Connector execution and supports private endpoints.
  - `direct` uses Scheduler's HTTP executor, with idempotency/window headers, bounded responses, HMAC signing and retry classification.

Definitions are configuration. A successful downstream call must return a durable receipt before a run is accepted.

Runtime-operation target semantics remain downstream-owner concerns. The
current shared authoring contract exposes scheduled Workflow dispatch and
Report snapshot refresh; governed report export runs through a scheduled
Workflow rather than letting Scheduler create Report records.

Run history and dead-letter queues stay Scheduler-owned in both deployment
topologies. Runtime operations consoles query them through the SDK `Binding`;
Runtime only authorizes and projects the response and never mirrors the rows as
business records.

## Repository layout

```text
cmd/
  scheduler-server/                     standalone Scheduler SaaS executable
internal/
  application/scheduler/                deployment-neutral scheduling use cases
  domain/scheduler/service/             Scheduler-owned retry and execution policies
  assembly/
    module/                             embedded Module composition
    saas/                               standalone multi-application composition
  adapter/
    http/                               direct-HTTP downstream adapter
    schedulersdk/                       Scheduler SDK callback-gateway adapter
  transport/http/saas/                  authenticated SaaS HTTP transport
  infrastructure/persistence/
    engine.go                           ORM engine selection at the composition boundary
    database/
      scheduler/                        definition, run, lease, event and dead-letter repositories
      migration/                        standalone migration-ledger coordinator
      schema/                           structured Scheduler-owned schema definitions
    mysql|postgres|sqlite/              database-specific ORM engines
module/                                 public in-process Module factory
remote/                                 public Scheduler SaaS client binding
```

Only `module` and `remote` are public Go library packages. The standalone
server is an executable; its assembly, adapters and transport stay below
`internal` so applications cannot couple to implementation details. Module
mode and SaaS mode assemble the same application service through
`internal/assembly/module`, while `internal/assembly/saas` owns process-level
application isolation and worker lifecycles.

Scheduler domain contracts shared with hosts live in `domainry-scheduler-sdk`;
this repository does not duplicate those models or repository interfaces under
`internal/domain`. Source-owned behavior such as retry calculation remains in
`internal/domain/scheduler/service`.

Persistence is source-owned by Scheduler. Module mode submits migrations to the
host registrar and therefore shares the host database, dialect, migration lock
and `_schema_migrations` ledger. SaaS mode applies the same source-owned schema
to the Scheduler service database and uses that database's single
`_schema_migrations` ledger. Repositories receive database and dialect ports;
they do not select engines or issue handwritten persistence SQL.

The deterministic recurrence functions live in
`domainry-scheduler-sdk/schedule` beside the shared `Schedule` contract and are
used by Module, SaaS, and Runtime compatibility paths.
