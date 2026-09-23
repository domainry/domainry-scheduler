package scheduler

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
	schedulerpersistence "github.com/domainry/domainry-scheduler-sdk/persistence"
	schedulerstore "github.com/domainry/domainry-scheduler/internal/infrastructure/persistence/database"
	definitionstore "github.com/domainry/domainry-scheduler/internal/testsupport/definitionstore"
	_ "modernc.org/sqlite"
)

type hostStub struct {
	definition     schedulersdk.Definition
	due            []modulehost.DueTrigger
	dispatched     int
	accepted       int
	lease          bool
	renewed        int
	loseLease      bool
	dispatchFn     func(context.Context) (schedulersdk.DownstreamReceipt, error)
	snapshot       schedulerpersistence.DefinitionSnapshot
	lastClaim      modulehost.DueTrigger
	lastTrigger    schedulersdk.Trigger
	syncs          int
	planDefinition schedulersdk.Definition
	planNext       time.Time
	planEnabled    bool
}

type blockingDefinitionRepository struct {
	mu           sync.Mutex
	calls        int
	firstEntered chan struct{}
	releaseFirst chan struct{}
	snapshot     schedulerpersistence.DefinitionSnapshot
	revisions    []int64
}

type blockingSnapshotReadRepository struct {
	inner    schedulerpersistence.DefinitionRepository
	revision int64
	loaded   chan struct{}
	release  chan struct{}
	once     sync.Once
}

type lifecycleHost struct {
	hostStub
	snapshots atomic.Int64
	started   chan struct{}
}

func newLifecycleHost() *lifecycleHost {
	return &lifecycleHost{
		hostStub: hostStub{definition: testServiceDefinition("lifecycle", "run-lifecycle")},
		started:  make(chan struct{}, 16),
	}
}

func (h *lifecycleHost) Definitions() modulehost.DefinitionProvider { return h }

func (h *lifecycleHost) Snapshot(context.Context) (schedulersdk.DefinitionSnapshot, error) {
	revision := h.snapshots.Add(1)
	h.started <- struct{}{}
	return schedulersdk.DefinitionSnapshot{Revision: revision, Definitions: []schedulersdk.Definition{h.definition}}, nil
}

func newLifecycleService(ctx context.Context, cancel context.CancelFunc, host *lifecycleHost) *Service {
	return NewService(ctx, cancel, schedulersdk.ApplicationRef{RuntimeID: "runtime-lifecycle"}, host, host, host, host, schedulersdk.DeploymentModeModule)
}

func waitLifecycleSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func waitLifecycleDone(t *testing.T, done <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func (r *blockingSnapshotReadRepository) SyncDefinitions(ctx context.Context, snapshot schedulerpersistence.DefinitionSnapshot) error {
	return r.inner.SyncDefinitions(ctx, snapshot)
}

func (r *blockingSnapshotReadRepository) DefinitionSnapshot(ctx context.Context) (schedulerpersistence.DefinitionSnapshot, error) {
	snapshot, err := r.inner.DefinitionSnapshot(ctx)
	if err != nil || snapshot.Revision != r.revision {
		return snapshot, err
	}
	r.once.Do(func() {
		close(r.loaded)
		<-r.release
	})
	return snapshot, nil
}

func (r *blockingDefinitionRepository) SyncDefinitions(_ context.Context, snapshot schedulerpersistence.DefinitionSnapshot) error {
	r.mu.Lock()
	r.calls++
	call := r.calls
	r.mu.Unlock()
	if call == 1 {
		close(r.firstEntered)
		<-r.releaseFirst
	}
	r.mu.Lock()
	r.snapshot = snapshot
	r.revisions = append(r.revisions, snapshot.Revision)
	r.mu.Unlock()
	return nil
}

func (r *blockingDefinitionRepository) DefinitionSnapshot(context.Context) (schedulerpersistence.DefinitionSnapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.snapshot, nil
}

func (h *hostStub) SyncDefinitions(_ context.Context, snapshot schedulerpersistence.DefinitionSnapshot) error {
	h.syncs++
	h.snapshot = snapshot
	return nil
}

func TestSaaSWorkerStartDoesNotPublishAnEmptyHostSnapshot(t *testing.T) {
	host := &hostStub{snapshot: schedulerpersistence.DefinitionSnapshot{
		SchemaVersion: "1", SourceKind: "runtime_host", SourceID: "runtime-a",
		Definitions: []schedulersdk.Definition{testServiceDefinition("persisted", "run-persisted")},
	}}
	ownerCtx, cancelOwner := context.WithCancel(t.Context())
	service := NewService(ownerCtx, cancelOwner, schedulersdk.ApplicationRef{RuntimeID: "runtime-a"}, host, host, host, host, schedulersdk.DeploymentModeSaaS)
	workerCtx, cancelWorker := context.WithCancel(t.Context())
	done := service.Start(workerCtx, schedulersdk.WorkerConfig{Enabled: true, PollInterval: time.Hour, BatchSize: 1})
	cancelWorker()
	<-done
	if host.syncs != 0 {
		t.Fatalf("SaaS worker start synchronized host snapshot %d times", host.syncs)
	}
}

// This is the Scheduler side of Runtime's controlled-worker contract: pause
// cancels one child context, waits for its done channel, and resume calls Start
// again on the same binding.
func TestModuleWorkerRestartsOnSameBindingAfterControlledPause(t *testing.T) {
	host := newLifecycleHost()
	ownerCtx, cancelOwner := context.WithCancel(t.Context())
	defer cancelOwner()
	var binding schedulersdk.Binding = newLifecycleService(ownerCtx, cancelOwner, host)
	config := schedulersdk.WorkerConfig{Enabled: true, PollInterval: time.Hour, BatchSize: 1}

	firstCtx, cancelFirst := context.WithCancel(t.Context())
	firstDone := binding.Start(firstCtx, config)
	waitLifecycleSignal(t, host.started, "first Scheduler worker start")
	cancelFirst()
	cancelFirst()
	waitLifecycleDone(t, firstDone, "first Scheduler worker stop")

	secondCtx, cancelSecond := context.WithCancel(t.Context())
	secondDone := binding.Start(secondCtx, config)
	select {
	case <-secondDone:
		t.Fatal("resumed Scheduler worker returned an already-closed done channel")
	default:
	}
	waitLifecycleSignal(t, host.started, "resumed Scheduler worker start")
	cancelSecond()
	waitLifecycleDone(t, secondDone, "resumed Scheduler worker stop")
	if firstDone == secondDone || host.snapshots.Load() != 2 {
		t.Fatalf("worker generations first=%p second=%p starts=%d", firstDone, secondDone, host.snapshots.Load())
	}
}

func TestModuleConcurrentStartJoinsOneWorkerGeneration(t *testing.T) {
	host := newLifecycleHost()
	ownerCtx, cancelOwner := context.WithCancel(t.Context())
	defer cancelOwner()
	service := newLifecycleService(ownerCtx, cancelOwner, host)
	workerCtx, cancelWorker := context.WithCancel(t.Context())
	config := schedulersdk.WorkerConfig{Enabled: true, PollInterval: time.Hour, BatchSize: 1}
	const callers = 16
	gate := make(chan struct{})
	results := make(chan (<-chan struct{}), callers)
	for range callers {
		go func() {
			<-gate
			results <- service.Start(workerCtx, config)
		}()
	}
	close(gate)
	var generation <-chan struct{}
	for range callers {
		done := <-results
		if generation == nil {
			generation = done
			continue
		}
		if done != generation {
			t.Fatal("concurrent Start did not join the active Scheduler worker generation")
		}
	}
	waitLifecycleSignal(t, host.started, "concurrent Scheduler worker start")
	if host.snapshots.Load() != 1 {
		t.Fatalf("concurrent Start created %d Scheduler loops", host.snapshots.Load())
	}
	cancelWorker()
	cancelWorker()
	waitLifecycleDone(t, generation, "concurrent Scheduler worker generation stop")
}

func TestModuleStartCloseRaceIsTerminalAndDoesNotLeakWorker(t *testing.T) {
	for attempt := range 64 {
		ownerCtx, cancelOwner := context.WithCancel(t.Context())
		host := newLifecycleHost()
		service := newLifecycleService(ownerCtx, cancelOwner, host)
		gate := make(chan struct{})
		started := make(chan (<-chan struct{}), 1)
		closed := make(chan error, 1)
		go func() {
			<-gate
			started <- service.Start(t.Context(), schedulersdk.WorkerConfig{Enabled: true, PollInterval: time.Hour, BatchSize: 1})
		}()
		go func() {
			<-gate
			closed <- service.Close(t.Context())
		}()
		close(gate)
		done := <-started
		if err := <-closed; err != nil {
			t.Fatalf("attempt %d close: %v", attempt, err)
		}
		waitLifecycleDone(t, done, "Start/Close race worker stop")
		if err := service.Close(t.Context()); err != nil {
			t.Fatalf("attempt %d repeated close: %v", attempt, err)
		}
		waitLifecycleDone(t, service.Start(t.Context(), schedulersdk.WorkerConfig{Enabled: true}), "post-Close Start")
	}
}

func TestModuleOwnerShutdownStopsWorkerAndRejectsRestart(t *testing.T) {
	host := newLifecycleHost()
	ownerCtx, cancelOwner := context.WithCancel(t.Context())
	service := newLifecycleService(ownerCtx, cancelOwner, host)
	done := service.Start(t.Context(), schedulersdk.WorkerConfig{Enabled: true, PollInterval: time.Hour, BatchSize: 1})
	waitLifecycleSignal(t, host.started, "Scheduler worker before owner shutdown")
	cancelOwner()
	waitLifecycleDone(t, done, "Scheduler worker owner shutdown")
	waitLifecycleDone(t, service.Start(t.Context(), schedulersdk.WorkerConfig{Enabled: true}), "post-shutdown Start")
}

func TestReconcileSnapshotSerializesDeterministicOverlapWithoutStaleLastWrite(t *testing.T) {
	host := &hostStub{}
	repository := &blockingDefinitionRepository{firstEntered: make(chan struct{}), releaseFirst: make(chan struct{})}
	ownerCtx, cancel := context.WithCancel(t.Context())
	service := NewService(ownerCtx, cancel, schedulersdk.ApplicationRef{RuntimeID: "runtime-a"}, host, host, host, repository, schedulersdk.DeploymentModeSaaS)
	session := schedulersdk.DefinitionPublisherSession{ContractVersion: schedulersdk.DefinitionPublicationContractVersion, Generation: 1, SessionNonce: "0123456789abcdef0123456789abcdef"}
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- service.ReconcileSnapshot(t.Context(), schedulersdk.DefinitionSnapshot{PublisherSession: &session, Revision: 1, Definitions: []schedulersdk.Definition{testServiceDefinition("shared", "revision-1")}})
	}()
	<-repository.firstEntered
	secondDone := make(chan error, 1)
	secondStarted := make(chan struct{})
	go func() {
		close(secondStarted)
		secondDone <- service.ReconcileSnapshot(t.Context(), schedulersdk.DefinitionSnapshot{PublisherSession: &session, Revision: 2, Definitions: []schedulersdk.Definition{testServiceDefinition("shared", "revision-2")}})
	}()
	<-secondStarted
	close(repository.releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	repository.mu.Lock()
	revisions := append([]int64(nil), repository.revisions...)
	repository.mu.Unlock()
	service.mu.RLock()
	definition := service.definitions["shared"]
	service.mu.RUnlock()
	if len(revisions) != 2 || revisions[0] != 1 || revisions[1] != 2 || definition.Target.Operation != "revision-2" {
		t.Fatalf("persisted revisions=%v final definition=%+v", revisions, definition)
	}
}

func TestSaaSProjectionCannotRegressAfterNewerCanonicalSnapshotCommits(t *testing.T) {
	database, err := sql.Open("sqlite", "file:scheduler-projection-fence?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := schedulerstore.EnsureSchema(t.Context(), database, "sqlite", ""); err != nil {
		t.Fatal(err)
	}
	dialect, err := schedulerstore.Renderer("sqlite", "")
	if err != nil {
		t.Fatal(err)
	}
	const runtimeID = "runtime-projection-fence"
	sharedDefinitions := definitionstore.New()
	canonicalA, err := schedulerstore.NewDefinitionStore(database, dialect, sharedDefinitions, runtimeID, schedulersdk.DeploymentModeSaaS)
	if err != nil {
		t.Fatal(err)
	}
	canonicalB, err := schedulerstore.NewDefinitionStore(database, dialect, sharedDefinitions, runtimeID, schedulersdk.DeploymentModeSaaS)
	if err != nil {
		t.Fatal(err)
	}
	runsA, err := schedulerstore.NewStore(database, dialect, runtimeID, "projection-worker-a")
	if err != nil {
		t.Fatal(err)
	}
	runsB, err := schedulerstore.NewStore(database, dialect, runtimeID, "projection-worker-b")
	if err != nil {
		t.Fatal(err)
	}
	readA := &blockingSnapshotReadRepository{inner: canonicalA, revision: 6, loaded: make(chan struct{}), release: make(chan struct{})}
	hostA, hostB := &hostStub{}, &hostStub{}
	ownerA, cancelA := context.WithCancel(t.Context())
	defer cancelA()
	ownerB, cancelB := context.WithCancel(t.Context())
	defer cancelB()
	serviceA := NewService(ownerA, cancelA, schedulersdk.ApplicationRef{RuntimeID: runtimeID}, hostA, hostA, runsA, readA, schedulersdk.DeploymentModeSaaS)
	serviceB := NewService(ownerB, cancelB, schedulersdk.ApplicationRef{RuntimeID: runtimeID}, hostB, hostB, runsB, canonicalB, schedulersdk.DeploymentModeSaaS)
	session, err := canonicalA.BeginDefinitionPublisherSession(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	snapshot := func(revision int64, suffix string) schedulersdk.DefinitionSnapshot {
		return schedulersdk.DefinitionSnapshot{PublisherSession: &session, Revision: revision, Definitions: []schedulersdk.Definition{
			testServiceDefinition("from-a", "operation-a-"+suffix),
			testServiceDefinition("from-b", "operation-b-"+suffix),
		}}
	}
	aDone := make(chan error, 1)
	go func() { aDone <- serviceA.ReconcileSnapshot(t.Context(), snapshot(6, "revision-6")) }()
	<-readA.loaded
	if err := serviceB.ReconcileSnapshot(t.Context(), snapshot(7, "revision-7")); err != nil {
		t.Fatal(err)
	}
	close(readA.release)
	if err := <-aDone; err != nil {
		t.Fatal(err)
	}

	canonical, err := canonicalA.DefinitionSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	wantFence, err := session.PublisherFence()
	if err != nil {
		t.Fatal(err)
	}
	wantContent, err := schedulersdk.DefinitionSnapshotContentSHA256(snapshot(7, "revision-7").Definitions)
	if err != nil {
		t.Fatal(err)
	}
	if canonical.Revision != 7 || canonical.PublisherFence == nil || !canonical.PublisherFence.Equal(wantFence) || canonical.ContentSHA256 != wantContent {
		t.Fatalf("canonical cursor revision=%d fence=%+v content=%s", canonical.Revision, canonical.PublisherFence, canonical.ContentSHA256)
	}
	for _, key := range []string{"from-a", "from-b"} {
		var raw string
		var projectionRevision int64
		if err := database.QueryRowContext(t.Context(), `SELECT definition_json, snapshot_revision FROM _scheduler_schedules WHERE runtime_id = ? AND schedule_id = ?`, runtimeID, key).Scan(&raw, &projectionRevision); err != nil {
			t.Fatal(err)
		}
		if projectionRevision != 7 || !strings.Contains(raw, "revision-7") {
			t.Fatalf("definition %s projection revision=%d payload=%s", key, projectionRevision, raw)
		}
	}
	runA, err := serviceA.TriggerNow(t.Context(), "from-a", "verify service A view")
	if err != nil {
		t.Fatal(err)
	}
	runB, err := serviceB.TriggerNow(t.Context(), "from-b", "verify service B view")
	if err != nil {
		t.Fatal(err)
	}
	if runA.Trigger.Target.Operation != "operation-a-revision-7" || runB.Trigger.Target.Operation != "operation-b-revision-7" {
		t.Fatalf("service A target=%q service B target=%q", runA.Trigger.Target.Operation, runB.Trigger.Target.Operation)
	}
}

func testServiceDefinition(key, operation string) schedulersdk.Definition {
	return schedulersdk.Definition{
		Key: key, Name: key, Revision: "v1", Status: "enabled",
		Schedule: schedulersdk.Schedule{Type: "interval", IntervalSeconds: 60},
		Target:   schedulersdk.TargetRef{Type: "runtime_operation", Owner: "workflow", Operation: operation},
	}
}
func (h *hostStub) DefinitionSnapshot(context.Context) (schedulerpersistence.DefinitionSnapshot, error) {
	return h.snapshot, nil
}

func (h *hostStub) Definitions() modulehost.DefinitionProvider         { return h }
func (h *hostStub) Runs() modulehost.RunStore                          { return h }
func (h *hostStub) Dispatcher() modulehost.Dispatcher                  { return h }
func (h *hostStub) HTTPConnections() modulehost.HTTPConnectionProvider { return h }
func (h *hostStub) Snapshot(context.Context) (schedulersdk.DefinitionSnapshot, error) {
	return schedulersdk.DefinitionSnapshot{Revision: 1, Definitions: []schedulersdk.Definition{h.definition}}, nil
}
func (*hostStub) Reconcile(context.Context, schedulersdk.Definition, time.Time) error { return nil }

func (h *hostStub) ReconcileScheduledPlan(_ context.Context, definition schedulersdk.Definition, next time.Time, enabled bool) error {
	h.planDefinition, h.planNext, h.planEnabled = definition, next, enabled
	return nil
}
func (*hostStub) DisableMissing(context.Context, []string, int64) error { return nil }
func (*hostStub) ApplyDefinitionSnapshot(context.Context, schedulerpersistence.DefinitionSnapshot, time.Time) (bool, error) {
	return true, nil
}
func (h *hostStub) Due(context.Context, time.Time, int) ([]modulehost.DueTrigger, error) {
	due := h.due
	h.due = nil
	return due, nil
}
func (h *hostStub) Claim(_ context.Context, due modulehost.DueTrigger, ttl time.Duration) (schedulersdk.Run, bool, error) {
	h.lastClaim = due
	windowKey := due.ScheduledFor.UTC().Format(time.RFC3339)
	run := schedulersdk.Run{Trigger: schedulersdk.Trigger{RunID: "run-1", DefinitionKey: due.Definition.Key, DefinitionRev: due.Definition.Revision, ScheduledFor: due.ScheduledFor, WindowKey: windowKey, Target: due.Definition.Target, IdempotencyKey: "scheduler:daily:window", Attempt: 1, Metadata: append(json.RawMessage(nil), due.Metadata...)}, Status: "leased"}
	if h.lease {
		run.Lease = schedulersdk.Lease{Owner: "worker-a", Token: 1, ExpiresAt: time.Now().Add(ttl)}
	}
	return run, true, nil
}
func (h *hostStub) Renew(_ context.Context, run schedulersdk.Run, _ time.Duration) (schedulersdk.Run, bool, error) {
	h.renewed++
	return run, !h.loseLease, nil
}
func (h *hostStub) Accept(context.Context, schedulersdk.Run, schedulersdk.DownstreamReceipt) error {
	h.accepted++
	return nil
}
func (*hostStub) Fail(context.Context, schedulersdk.Run, error, time.Time) error { return nil }
func (*hostStub) List(context.Context, int) ([]schedulersdk.Run, error)          { return nil, nil }
func (*hostStub) Get(context.Context, string) (schedulersdk.Run, error) {
	return schedulersdk.Run{}, nil
}
func (*hostStub) Retry(context.Context, string, string) (schedulersdk.Run, error) {
	return schedulersdk.Run{}, nil
}
func (*hostStub) Cancel(context.Context, string, string) (schedulersdk.Run, error) {
	return schedulersdk.Run{}, nil
}
func (*hostStub) DeadLetters(context.Context, int) ([]schedulersdk.DeadLetter, error) {
	return nil, nil
}
func (*hostStub) DeadLetter(context.Context, string) (schedulersdk.DeadLetter, error) {
	return schedulersdk.DeadLetter{}, nil
}
func (*hostStub) ResolveDeadLetter(context.Context, string, string) (schedulersdk.DeadLetter, error) {
	return schedulersdk.DeadLetter{}, nil
}
func (*hostStub) RequeueDeadLetter(context.Context, string, string) (schedulersdk.Run, error) {
	return schedulersdk.Run{}, nil
}
func (*hostStub) Reschedule(context.Context, string, time.Time, string) error { return nil }
func (h *hostStub) Dispatch(ctx context.Context, trigger schedulersdk.Trigger) (schedulersdk.DownstreamReceipt, error) {
	h.dispatched++
	h.lastTrigger = trigger
	if h.dispatchFn != nil {
		return h.dispatchFn(ctx)
	}
	return schedulersdk.DownstreamReceipt{ID: "downstream-1", Owner: "integration", Status: "accepted"}, nil
}

func TestTriggerNowKeepsRealClockCompatibility(t *testing.T) {
	definition := schedulersdk.Definition{Key: "daily", Status: "enabled", Revision: "v1", Schedule: schedulersdk.Schedule{Type: "interval", IntervalSeconds: 60}, Target: schedulersdk.TargetRef{Type: "runtime_operation", Owner: "workflow", Operation: "run"}}
	host := &hostStub{definition: definition}
	ownerCtx, cancel := context.WithCancel(t.Context())
	service := NewService(ownerCtx, cancel, schedulersdk.ApplicationRef{RuntimeID: "runtime-a"}, host, host, host, host, schedulersdk.DeploymentModeModule)
	if err := service.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	before := time.Now().UTC()
	run, err := service.TriggerNow(t.Context(), "daily", "ordinary production run")
	after := time.Now().UTC()
	if err != nil || run.Status != "leased" || run.DownstreamReceipt.ID != "" || host.dispatched != 1 || host.accepted != 1 || run.Trigger.ScheduledFor.Before(before) || run.Trigger.ScheduledFor.After(after) {
		t.Fatalf("run=%+v claim=%+v err=%v", run, host.lastClaim, err)
	}
}

func TestDispatchCancelsWhenDatabaseLeaseIsLost(t *testing.T) {
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	definition := schedulersdk.Definition{Key: "leased", Status: "enabled", Revision: "v1", Schedule: schedulersdk.Schedule{Type: "interval", IntervalSeconds: 60}, Target: schedulersdk.TargetRef{Type: "runtime_operation", Owner: "workflow", Operation: "run"}}
	host := &hostStub{definition: definition, due: []modulehost.DueTrigger{{Definition: definition, ScheduledFor: now}}, lease: true, loseLease: true}
	host.dispatchFn = func(ctx context.Context) (schedulersdk.DownstreamReceipt, error) {
		<-ctx.Done()
		return schedulersdk.DownstreamReceipt{}, ctx.Err()
	}
	ownerCtx, cancel := context.WithCancel(t.Context())
	apiBinding := NewService(ownerCtx, cancel, schedulersdk.ApplicationRef{RuntimeID: "runtime-a"}, host, host, host, host, schedulersdk.DeploymentModeModule)
	concrete := apiBinding
	concrete.mu.Lock()
	concrete.leaseTTL = 15 * time.Millisecond
	concrete.mu.Unlock()
	_, err := apiBinding.Tick(t.Context(), now, 1)
	if err == nil || host.renewed == 0 || host.accepted != 0 {
		t.Fatalf("err=%v renewed=%d accepted=%d", err, host.renewed, host.accepted)
	}
}

func TestDispatchUsesSchedulerOwnerTimeoutWhenDefinitionHasNone(t *testing.T) {
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	definition := testServiceDefinition("bounded", "run")
	host := &hostStub{definition: definition, due: []modulehost.DueTrigger{{Definition: definition, ScheduledFor: now}}}
	host.dispatchFn = func(ctx context.Context) (schedulersdk.DownstreamReceipt, error) {
		<-ctx.Done()
		return schedulersdk.DownstreamReceipt{}, ctx.Err()
	}
	ownerCtx, cancel := context.WithCancel(t.Context())
	service := NewService(ownerCtx, cancel, schedulersdk.ApplicationRef{RuntimeID: "runtime-a"}, host, host, host, host, schedulersdk.DeploymentModeModule)
	service.mu.Lock()
	service.dispatchTimeout = 25 * time.Millisecond
	service.mu.Unlock()
	started := time.Now()
	processed, err := service.Tick(t.Context(), now, 1)
	if !errors.Is(err, context.DeadlineExceeded) || processed != 0 || time.Since(started) > time.Second || host.accepted != 0 {
		t.Fatalf("processed=%d elapsed=%s accepted=%d err=%v", processed, time.Since(started), host.accepted, err)
	}
}
func (*hostStub) ResolveHTTPConnection(context.Context, string) (modulehost.HTTPConnection, error) {
	return modulehost.HTTPConnection{}, nil
}

func TestModuleOwnsClockButRoutesRuntimeCallbackToHostDispatcher(t *testing.T) {
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	definition := schedulersdk.Definition{Key: "partner_sync", Name: "Partner sync", Status: "enabled", Revision: "v1", Schedule: schedulersdk.Schedule{Type: "interval", IntervalSeconds: 60}, Target: schedulersdk.TargetRef{Type: "http", ConnectionKey: "partner", Operation: "sync", DispatchMode: "runtime_callback"}}
	host := &hostStub{definition: definition, due: []modulehost.DueTrigger{{Definition: definition, ScheduledFor: now}}}
	ownerCtx, cancel := context.WithCancel(t.Context())
	binding := NewService(ownerCtx, cancel, schedulersdk.ApplicationRef{RuntimeID: "runtime-a"}, host, host, host, host, schedulersdk.DeploymentModeModule)
	if err := binding.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	processed, err := binding.Tick(t.Context(), now, 10)
	if err != nil || processed != 1 || host.dispatched != 1 || host.accepted != 1 {
		t.Fatalf("processed=%d dispatched=%d accepted=%d err=%v", processed, host.dispatched, host.accepted, err)
	}
}
