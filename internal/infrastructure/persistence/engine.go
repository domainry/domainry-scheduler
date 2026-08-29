package persistence

import (
	"fmt"
	"strings"

	ormdialect "github.com/domainry/domainry-orm/dialect"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
	mysqlstore "github.com/domainry/domainry-scheduler/internal/infrastructure/persistence/mysql"
	postgresstore "github.com/domainry/domainry-scheduler/internal/infrastructure/persistence/postgres"
	sqlitestore "github.com/domainry/domainry-scheduler/internal/infrastructure/persistence/sqlite"
)

type DatabaseEngine interface {
	Renderer(string) modulehost.Dialect
}

var databaseEngineFactories = map[ormdialect.Name]func() DatabaseEngine{
	ormdialect.SQLite:   func() DatabaseEngine { return sqlitestore.NewEngine() },
	ormdialect.Postgres: func() DatabaseEngine { return postgresstore.NewEngine() },
	ormdialect.MySQL:    func() DatabaseEngine { return mysqlstore.NewEngine() },
}

func NewEngine(driver string) (DatabaseEngine, error) {
	name := strings.ToLower(strings.TrimSpace(driver))
	switch name {
	case "sqlite3":
		name = "sqlite"
	case "postgresql", "pgx":
		name = "postgres"
	}
	dialect, err := ormdialect.Parse(name)
	if err != nil {
		return nil, fmt.Errorf("Scheduler database driver %q is unsupported: %w", driver, err)
	}
	factory := databaseEngineFactories[dialect.Name()]
	if factory == nil {
		return nil, fmt.Errorf("Scheduler database driver %q is unsupported", driver)
	}
	return factory(), nil
}
