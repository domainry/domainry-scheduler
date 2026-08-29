package persistence

import (
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
	storeschema "github.com/domainry/domainry-scheduler/internal/infrastructure/persistence/database/schema"
)

const SchemaVersion = storeschema.SchemaVersion

func SchemaMigrations(driver, databaseSchema string) ([]modulehost.SchemaMigration, error) {
	engine, err := NewEngine(driver)
	if err != nil {
		return nil, err
	}
	return storeschema.Migrations(engine.Renderer(databaseSchema))
}

func Renderer(driver, databaseSchema string) (modulehost.Dialect, error) {
	engine, err := NewEngine(driver)
	if err != nil {
		return nil, err
	}
	return engine.Renderer(databaseSchema), nil
}
