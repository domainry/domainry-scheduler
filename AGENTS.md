# Development rules

- Answer architecture and implementation questions from the current repository code and tests, not from memory.
- Keep public packages limited to stable host-facing entry points. Application, domain, assembly, adapter, transport, and infrastructure implementations belong under `internal`.
- `module` and `remote` are peer adapters and must not depend on one another.
- Every database has exactly one host-owned migration ledger named `_schema_migrations`. Embedded Module migrations must be submitted through the host migration registrar; standalone SaaS migrations use the Scheduler database ledger.
- Persistence DDL and DML must use `github.com/domainry/domainry-orm`. Raw SQL is allowed only when the ORM has no equivalent, with a local justification and dialect-focused tests.
- Embedded Module mode uses the host database, transaction boundary, SQL dialect, migration lock, and migration ledger. Scheduler remains the business owner of its schema and state.
- Develop in the existing checkout; do not create a worktree for repository changes.
