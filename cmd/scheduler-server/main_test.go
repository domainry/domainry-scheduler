package main

import (
	"strings"
	"testing"
	"time"
)

func TestConfigurationFromEnvironmentBuildsSQLiteDefaults(t *testing.T) {
	t.Setenv("SCHEDULER_DATABASE_DRIVER", "sqlite")
	t.Setenv("SCHEDULER_DATABASE_DSN", "")
	t.Setenv("SCHEDULER_WORKER_ID", "worker-a")
	t.Setenv("SCHEDULER_RUNTIME_ID", "runtime-a")
	t.Setenv("SCHEDULER_SAAS_TOKEN", "control-token")
	t.Setenv("SCHEDULER_RUNTIME_ENDPOINT", "https://runtime.example")
	t.Setenv("SCHEDULER_RUNTIME_SIGNING_SECRET", "runtime-signing-secret")
	config, err := configurationFromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	if config.sqlDriver != "sqlite" || config.storeDriver != "sqlite" || config.databaseDSN != "scheduler.db" || config.httpAddress != ":8080" || config.maxPendingTriggers != 10_000 || config.dispatchTimeout != 5*time.Minute {
		t.Fatalf("unexpected configuration: %+v", config)
	}
	if _, err := newDispatchGateway(config); err != nil {
		t.Fatalf("build Runtime-bound dispatch gateway: %v", err)
	}
}

func TestConfigurationFromEnvironmentValidatesCapacityAndDispatchTimeout(t *testing.T) {
	for key, value := range map[string]string{
		"SCHEDULER_DATABASE_DRIVER": "sqlite", "SCHEDULER_WORKER_ID": "worker-a", "SCHEDULER_RUNTIME_ID": "runtime-a",
		"SCHEDULER_SAAS_TOKEN": "token", "SCHEDULER_RUNTIME_ENDPOINT": "https://runtime.example", "SCHEDULER_RUNTIME_SIGNING_SECRET": "secret",
		"SCHEDULER_MAX_PENDING_TRIGGERS": "77", "SCHEDULER_DISPATCH_TIMEOUT": "45s",
	} {
		t.Setenv(key, value)
	}
	config, err := configurationFromEnvironment()
	if err != nil || config.maxPendingTriggers != 77 || config.dispatchTimeout != 45*time.Second {
		t.Fatalf("config=%+v err=%v", config, err)
	}
	t.Setenv("SCHEDULER_MAX_PENDING_TRIGGERS", "0")
	if _, err = configurationFromEnvironment(); err == nil {
		t.Fatal("zero trigger backlog limit accepted")
	}
	t.Setenv("SCHEDULER_MAX_PENDING_TRIGGERS", "77")
	t.Setenv("SCHEDULER_DISPATCH_TIMEOUT", "31m")
	if _, err = configurationFromEnvironment(); err == nil {
		t.Fatal("unbounded dispatch timeout accepted")
	}
}

func TestConfigurationFromEnvironmentRejectsIncompleteProductionConfig(t *testing.T) {
	t.Setenv("SCHEDULER_DATABASE_DRIVER", "postgres")
	t.Setenv("SCHEDULER_DATABASE_DSN", "")
	t.Setenv("SCHEDULER_WORKER_ID", "")
	t.Setenv("SCHEDULER_RUNTIME_ID", "")
	t.Setenv("SCHEDULER_SAAS_TOKEN", "")
	t.Setenv("SCHEDULER_RUNTIME_ENDPOINT", "")
	t.Setenv("SCHEDULER_RUNTIME_SIGNING_SECRET", "")
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
