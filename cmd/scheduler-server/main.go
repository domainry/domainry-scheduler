package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"

	"github.com/domainry/domainry-foundation/logging"
	"github.com/domainry/domainry-foundation/processhost"
	ormsqlite "github.com/domainry/domainry-orm/sqlite"
	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/dispatchgateway"
	schedulersdkadapter "github.com/domainry/domainry-scheduler/internal/adapter/schedulersdk"
	saasassembly "github.com/domainry/domainry-scheduler/internal/assembly/saas"
	saashttp "github.com/domainry/domainry-scheduler/internal/transport/http/saas"
)

func main() {
	if run() != nil {
		os.Exit(1)
	}
}

func run() (runErr error) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	observability, err := processhost.Open(ctx, processhost.Config{ServiceName: "domainry-scheduler"})
	if err != nil {
		return err
	}
	started := false
	startupFailure := logging.Failure{Stage: "configuration_load"}
	defer func() {
		if !started && runErr != nil {
			observability.StartFailed(context.Background(), runErr, startupFailure)
			runErr = errors.Join(runErr, observability.Close(context.Background(), nil))
			return
		}
		runErr = observability.Finish(context.Background(), nil, runErr)
	}()
	config, err := configurationFromEnvironment()
	if err != nil {
		return err
	}
	startupFailure = logging.Failure{Stage: "database_initialize", Dependency: "scheduler_database"}
	database, err := openOwnedDatabase(ctx, config)
	if err != nil {
		return err
	}
	defer database.Close()
	startupFailure = logging.Failure{Stage: "dispatch_gateway_initialize", Dependency: "runtime"}
	gateway, err := newDispatchGateway(config)
	if err != nil {
		return err
	}
	startupFailure = logging.Failure{Stage: "module_assembly"}
	service, err := saasassembly.NewDatabaseService(saasassembly.DatabaseServiceOptions{
		Context: ctx, Database: database, Driver: config.storeDriver, Schema: config.databaseSchema,
		WorkerID: config.workerID, Worker: schedulersdk.WorkerConfig{Enabled: true, PollInterval: config.workerPollInterval, BatchSize: config.workerBatchSize, MaxPendingTriggers: config.maxPendingTriggers, DispatchTimeout: config.dispatchTimeout},
		Applications: []schedulersdk.ApplicationRef{{RuntimeID: config.runtimeID}},
		Downstreams:  schedulersdkadapter.RemoteDownstreams(gateway),
	})
	if err != nil {
		return err
	}
	defer service.Shutdown(context.Background())
	mux := http.NewServeMux()
	mux.HandleFunc("GET /live", func(response http.ResponseWriter, _ *http.Request) { response.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("GET /ready", func(response http.ResponseWriter, _ *http.Request) { response.WriteHeader(http.StatusNoContent) })
	startupFailure = logging.Failure{Stage: "http_handler_initialize"}
	schedulerHandler, err := saashttp.New(saashttp.Options{ApplicationTokens: map[string]string{config.runtimeID: config.bearerToken}, Service: service})
	if err != nil {
		return err
	}
	mux.Handle("/", schedulerHandler)
	httpServer := &http.Server{
		Addr: config.httpAddress, Handler: observability.Handler("public", mux), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second,
	}
	startupFailure = logging.Failure{Stage: "listener_bind", Dependency: "public_listener"}
	listener, err := net.Listen("tcp", config.httpAddress)
	if err != nil {
		return err
	}
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- httpServer.Serve(listener) }()
	started = true
	observability.Started(ctx, map[string]any{"listener_group": "public"})
	select {
	case <-ctx.Done():
		observability.Stopping(context.Background(), map[string]any{"reason": "signal"})
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return httpServer.Shutdown(shutdownCtx)
	case err := <-serveErrors:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func newDispatchGateway(config configuration) (*dispatchgateway.Remote, error) {
	return dispatchgateway.NewRemote(dispatchgateway.RemoteConfig{
		BaseURL: config.runtimeEndpoint, RuntimeID: config.runtimeID, SigningSecret: config.runtimeSigningSecret,
		RequestTimeout: config.gatewayTimeout, MaxAttempts: config.gatewayAttempts,
	})
}

type configuration struct {
	httpAddress, sqlDriver, storeDriver, databaseDSN, databaseSchema        string
	workerID, runtimeID, bearerToken, runtimeEndpoint, runtimeSigningSecret string
	databaseMaxOpen, databaseMaxIdle, workerBatchSize, maxPendingTriggers   int
	databaseConnLifetime, databaseLockTimeout, workerPollInterval           time.Duration
	gatewayTimeout, dispatchTimeout                                         time.Duration
	gatewayAttempts                                                         int
}

func configurationFromEnvironment() (configuration, error) {
	value := configuration{
		httpAddress: env("SCHEDULER_HTTP_ADDRESS", ":8080"),
		databaseDSN: strings.TrimSpace(os.Getenv("SCHEDULER_DATABASE_DSN")), databaseSchema: strings.TrimSpace(os.Getenv("SCHEDULER_DATABASE_SCHEMA")),
		workerID: strings.TrimSpace(os.Getenv("SCHEDULER_WORKER_ID")), runtimeID: strings.TrimSpace(os.Getenv("SCHEDULER_RUNTIME_ID")), bearerToken: strings.TrimSpace(os.Getenv("SCHEDULER_SAAS_TOKEN")),
		runtimeEndpoint: strings.TrimSpace(os.Getenv("SCHEDULER_RUNTIME_ENDPOINT")), runtimeSigningSecret: strings.TrimSpace(os.Getenv("SCHEDULER_RUNTIME_SIGNING_SECRET")),
		databaseMaxOpen: 20, databaseMaxIdle: 10, databaseConnLifetime: 30 * time.Minute, databaseLockTimeout: 5 * time.Second,
		workerPollInterval: time.Second, workerBatchSize: 100, maxPendingTriggers: 10_000, gatewayTimeout: 10 * time.Second, dispatchTimeout: 5 * time.Minute, gatewayAttempts: 3,
	}
	switch strings.ToLower(env("SCHEDULER_DATABASE_DRIVER", "sqlite")) {
	case "sqlite":
		value.storeDriver, value.sqlDriver = "sqlite", "sqlite"
		if value.databaseDSN == "" {
			value.databaseDSN = "scheduler.db"
		}
	case "postgres", "postgresql", "pgx":
		value.storeDriver, value.sqlDriver = "postgres", "pgx"
	case "mysql":
		value.storeDriver, value.sqlDriver = "mysql", "mysql"
	default:
		return configuration{}, fmt.Errorf("SCHEDULER_DATABASE_DRIVER must be sqlite, postgres or mysql")
	}
	if raw := strings.TrimSpace(os.Getenv("SCHEDULER_WORKER_BATCH_SIZE")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			return configuration{}, fmt.Errorf("SCHEDULER_WORKER_BATCH_SIZE must be positive")
		}
		value.workerBatchSize = parsed
	}
	if raw := strings.TrimSpace(os.Getenv("SCHEDULER_MAX_PENDING_TRIGGERS")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 1_000_000 {
			return configuration{}, fmt.Errorf("SCHEDULER_MAX_PENDING_TRIGGERS must be between 1 and 1000000")
		}
		value.maxPendingTriggers = parsed
	}
	if raw := strings.TrimSpace(os.Getenv("SCHEDULER_DISPATCH_TIMEOUT")); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil || parsed <= 0 || parsed > 30*time.Minute {
			return configuration{}, fmt.Errorf("SCHEDULER_DISPATCH_TIMEOUT must be between 1ns and 30m")
		}
		value.dispatchTimeout = parsed
	}
	if value.databaseDSN == "" || value.workerID == "" || value.runtimeID == "" || value.bearerToken == "" || value.runtimeEndpoint == "" || value.runtimeSigningSecret == "" {
		return configuration{}, fmt.Errorf("Scheduler SaaS database, worker, Runtime identity, bearer token and Runtime callback configuration are required")
	}
	return value, nil
}

func openOwnedDatabase(ctx context.Context, config configuration) (*sql.DB, error) {
	dsn := config.databaseDSN
	var sqliteConfig ormsqlite.OwnedConnectionConfig
	if config.storeDriver == "sqlite" {
		normalized := strings.ToLower(strings.TrimSpace(dsn))
		if normalized == ":memory:" || strings.Contains(normalized, "mode=memory") {
			return nil, fmt.Errorf("Scheduler SQLite requires a file database")
		}
		sqliteConfig = ormsqlite.DefaultOwnedConnectionConfig(dsn)
		sqliteConfig.BusyTimeout = config.databaseLockTimeout
		sqliteConfig.MaxOpenConnections = config.databaseMaxOpen
		sqliteConfig.MaxIdleConnections = config.databaseMaxIdle
		resolved, err := sqliteConfig.DSN()
		if err != nil {
			return nil, fmt.Errorf("configure Scheduler SQLite database: %w", err)
		}
		dsn = resolved
	}
	database, err := sql.Open(config.sqlDriver, dsn)
	if err != nil {
		return nil, fmt.Errorf("open Scheduler SaaS database: %w", err)
	}
	fail := func(err error) (*sql.DB, error) {
		_ = database.Close()
		return nil, err
	}
	if config.storeDriver == "sqlite" {
		if err := ormsqlite.InitializeOwned(ctx, database, sqliteConfig); err != nil {
			return fail(fmt.Errorf("initialize Scheduler SQLite database: %w", err))
		}
	} else {
		database.SetMaxOpenConns(config.databaseMaxOpen)
		database.SetMaxIdleConns(config.databaseMaxIdle)
	}
	database.SetConnMaxLifetime(config.databaseConnLifetime)
	if err := database.PingContext(ctx); err != nil {
		return fail(fmt.Errorf("ping Scheduler SaaS database: %w", err))
	}
	return database, nil
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
