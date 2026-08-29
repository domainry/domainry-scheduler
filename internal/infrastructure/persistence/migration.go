package persistence

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	ormbuilder "github.com/domainry/domainry-orm/builder"
)

// EnsureSchema applies Scheduler-owned migrations for a standalone SaaS
// database. Embedded Module deployments instead hand SchemaMigrations to the
// host ledger and never create this service-owned ledger.
func EnsureSchema(ctx context.Context, db *sql.DB, driver, schema string) error {
	if db == nil {
		return fmt.Errorf("Scheduler database is required")
	}
	r, err := newRenderer(driver, schema)
	if err != nil {
		return err
	}
	statement, _, err := ormbuilder.NewCreateTableBuilder(r.Renderer, "_schema_migrations").IfNotExists().WithoutSystemColumns().Columns(
		required("version", ormbuilder.BigIntType()), required("name", ormbuilder.TextKeyType(191)),
		required("checksum", ormbuilder.TextKeyType(64)), required("dirty", ormbuilder.BooleanType()),
		required("applied_at", ormbuilder.TextKeyType(40)),
	).PrimaryKey("version").Build()
	if err != nil {
		return fmt.Errorf("build Scheduler migration ledger: %w", err)
	}
	if _, err := db.ExecContext(ctx, statement); err != nil {
		return fmt.Errorf("prepare Scheduler migration ledger: %w", err)
	}
	migrations, err := SchemaMigrations(driver, schema)
	if err != nil {
		return err
	}
	for _, migration := range migrations {
		checksum := migrationChecksum(migration.Version, migration.Name, migration.Statements)
		query, queryArgs, err := ormbuilder.NewSelectBuilder(r.Renderer, "_schema_migrations").Columns("checksum", "dirty").Where(ormbuilder.Equal("version", migration.Version)).Build()
		if err != nil {
			return err
		}
		var applied string
		var dirty bool
		err = db.QueryRowContext(ctx, query, queryArgs...).Scan(&applied, &dirty)
		if err == nil {
			if dirty {
				return fmt.Errorf("scheduler migration %d is dirty", migration.Version)
			}
			if applied != checksum {
				return fmt.Errorf("scheduler migration %d checksum drift", migration.Version)
			}
			continue
		}
		if err != sql.ErrNoRows {
			return fmt.Errorf("inspect Scheduler migration %d: %w", migration.Version, err)
		}
		insert, insertArgs, err := ormbuilder.NewInsertBuilder(r.Renderer, "_schema_migrations").Columns("version", "name", "checksum", "dirty", "applied_at").Values(migration.Version, migration.Name, checksum, true, "").Build()
		if err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, insert, insertArgs...); err != nil {
			if !isUnique(err) {
				return err
			}
			// A peer won the migration row. Wait for the exact checksum to
			// become clean; a crashed owner leaves dirty=true and fails closed.
			if err := waitForMigration(ctx, db, query, queryArgs, migration.Version, checksum); err != nil {
				return err
			}
			continue
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		for _, ddl := range migration.Statements {
			if _, err := tx.ExecContext(ctx, ddl); err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("apply Scheduler migration %d: %w", migration.Version, err)
			}
		}
		complete, completeArgs, err := ormbuilder.NewUpdateBuilder(r.Renderer, "_schema_migrations").Set("dirty", false).Set("applied_at", time.Now().UTC().Format(time.RFC3339Nano)).Where(ormbuilder.And(ormbuilder.Equal("version", migration.Version), ormbuilder.Equal("checksum", checksum))).Build()
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		if _, err := tx.ExecContext(ctx, complete, completeArgs...); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func waitForMigration(ctx context.Context, db *sql.DB, query string, args []any, version uint, checksum string) error {
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		var applied string
		var dirty bool
		err := db.QueryRowContext(ctx, query, args...).Scan(&applied, &dirty)
		if err == nil && applied != checksum {
			return fmt.Errorf("scheduler migration %d checksum drift", version)
		}
		if err == nil && !dirty {
			return nil
		}
		if err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("inspect concurrent Scheduler migration %d: %w", version, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("scheduler migration %d remained dirty", version)
		case <-ticker.C:
		}
	}
}

func migrationChecksum(version uint, name string, statements []string) string {
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "%d\x00%s\x00%s", version, strings.TrimSpace(name), strings.Join(statements, "\x00"))
	return hex.EncodeToString(h.Sum(nil))
}
