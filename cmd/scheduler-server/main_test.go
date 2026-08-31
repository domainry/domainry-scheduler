package main

import (
	"strings"
	"testing"
)

func TestConfigurationFromEnvironmentBuildsSQLiteDefaults(t *testing.T) {
	t.Setenv("SCHEDULER_DATABASE_DRIVER", "sqlite")
	t.Setenv("SCHEDULER_DATABASE_DSN", "")
	t.Setenv("SCHEDULER_WORKER_ID", "worker-a")
	t.Setenv("SCHEDULER_SAAS_TOKEN", "control-token")
	t.Setenv("SCHEDULER_RUNTIME_ENDPOINT", "https://runtime.example")
	t.Setenv("SCHEDULER_RUNTIME_SERVICE_CREDENTIAL", "runtime-token")
	config, err := configurationFromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	if config.sqlDriver != "sqlite" || config.storeDriver != "sqlite" || config.databaseDSN != "scheduler.db" || config.httpAddress != ":8080" {
		t.Fatalf("unexpected configuration: %+v", config)
	}
}

func TestConfigurationFromEnvironmentRejectsIncompleteProductionConfig(t *testing.T) {
	t.Setenv("SCHEDULER_DATABASE_DRIVER", "postgres")
	t.Setenv("SCHEDULER_DATABASE_DSN", "")
	t.Setenv("SCHEDULER_WORKER_ID", "")
	t.Setenv("SCHEDULER_SAAS_TOKEN", "")
	t.Setenv("SCHEDULER_RUNTIME_ENDPOINT", "")
	t.Setenv("SCHEDULER_RUNTIME_SERVICE_CREDENTIAL", "")
	_, err := configurationFromEnvironment()
	if err == nil || !strings.Contains(err.Error(), "configuration are required") {
		t.Fatalf("err=%v", err)
	}
}

func TestOpenOwnedDatabaseRejectsInMemorySQLite(t *testing.T) {
	_, err := openOwnedDatabase(t.Context(), configuration{storeDriver: "sqlite", sqlDriver: "sqlite", databaseDSN: ":memory:"})
	if err == nil || !strings.Contains(err.Error(), "requires a file database") {
		t.Fatalf("err=%v", err)
	}
}
