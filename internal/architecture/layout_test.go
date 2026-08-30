package architecture

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestRepositoryRootContainsOnlyReviewedPackages(t *testing.T) {
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve architecture test path")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
	allowed := map[string]bool{"admin": true, "internal": true, "module": true, "remote": true, "server": true}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !entry.IsDir() || strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		if !allowed[entry.Name()] {
			t.Errorf("unreviewed Scheduler root directory %q; implementation packages belong below internal", entry.Name())
		}
	}
}

func TestModuleUsesTaggedDependencies(t *testing.T) {
	content, err := os.ReadFile("../../go.mod")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(content), "replace ") || strings.Contains(string(content), "../domainry-") {
		t.Fatal("Scheduler must consume released module tags, not local directory replacements")
	}
}

func TestSchedulerOwnsDurableStateInsteadOfBorrowingRunStore(t *testing.T) {
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve architecture test path")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
	factory, err := os.ReadFile(filepath.Join(root, "module", "factory.go"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(factory)
	for _, required := range []string{"SchemaMigrations", "ApplyOwnedMigrations", "NewStore"} {
		if !strings.Contains(text, required) {
			t.Errorf("Scheduler Module factory does not own %s", required)
		}
	}
	if strings.Contains(text, "host.Runs()") {
		t.Error("Scheduler Module must not borrow durable RunStore from Runtime")
	}
	for _, forbidden := range []string{"sql.Open", "InitializeOwned", "SetMaxOpenConns", "PRAGMA"} {
		if strings.Contains(text, forbidden) {
			t.Errorf("Scheduler Module must reuse the host pool instead of initializing it through %q", forbidden)
		}
	}
	for _, required := range []string{
		"internal/infrastructure/persistence/schema.go",
		"internal/infrastructure/persistence/database/store.go",
		"internal/infrastructure/persistence/database/schedule/store.go",
	} {
		if info, err := os.Stat(filepath.Join(root, filepath.FromSlash(required))); err != nil || info.IsDir() {
			t.Errorf("source-owned Scheduler persistence %q is missing", required)
		}
	}
}

func TestSchedulerPersistenceUsesDomainryORM(t *testing.T) {
	_, source, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(source), "..", "infrastructure", "persistence"))
	for _, name := range []string{"engine.go", "migration.go", "database/schema/migrations.go", "database/schedule/store.go"} {
		raw, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		text := string(raw)
		if !strings.Contains(text, "github.com/domainry/domainry-orm/") {
			t.Errorf("Scheduler persistence %s bypasses domainry-orm", name)
		}
		for _, rawSQL := range []string{`"SELECT `, `"INSERT INTO `, `"UPDATE `, `"DELETE FROM `, `"CREATE TABLE `} {
			if strings.Contains(text, rawSQL) {
				t.Errorf("Scheduler persistence %s contains handwritten SQL %s", name, rawSQL)
			}
		}
	}
}

func TestScheduleRepositoryReceivesDatabaseAndDialectPorts(t *testing.T) {
	_, source, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(source), "..", "infrastructure", "persistence"))
	raw, err := os.ReadFile(filepath.Join(root, "database", "schedule", "store.go"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	for _, forbidden := range []string{"*sql.DB", "ormdialect.Parse", "newRenderer", "driver string", "schema string"} {
		if strings.Contains(text, forbidden) {
			t.Errorf("Scheduler repository selects database infrastructure through %q", forbidden)
		}
	}
	for _, required := range []string{"modulehost.Database", "modulehost.Dialect"} {
		if !strings.Contains(text, required) {
			t.Errorf("Scheduler repository does not consume %s", required)
		}
	}
}

func TestSchedulerDatabaseChoiceIsConfinedToEngineFactoryAndProfiles(t *testing.T) {
	_, source, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(source), ".."))
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return err
		}
		normalized := filepath.ToSlash(path)
		if filepath.Base(path) == "engine.go" || strings.Contains(normalized, "/persistence/sqlite/") || strings.Contains(normalized, "/persistence/mysql/") || strings.Contains(normalized, "/persistence/postgres/") {
			return nil
		}
		content, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		text := strings.ToLower(string(content))
		for _, database := range []string{"sqlite", "sqlite3", "mysql", "postgres", "postgresql", "pgx"} {
			if strings.Contains(text, database) {
				t.Errorf("Scheduler database %q escaped engine/profile boundary: %s", database, path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
