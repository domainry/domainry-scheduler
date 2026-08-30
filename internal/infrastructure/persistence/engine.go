package persistence

import (
	"fmt"

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
	dialect, err := ormdialect.Parse(driver)
	if err != nil {
		return nil, fmt.Errorf("Scheduler database driver %q is unsupported: %w", driver, err)
	}
	factory := databaseEngineFactories[dialect.Name()]
	if factory == nil {
		return nil, fmt.Errorf("Scheduler database driver %q is unsupported", driver)
	}
	return factory(), nil
}
