package remote

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/domainry/domainry-foundation/modulecapability"
	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/modulehost"
	"github.com/domainry/domainry-scheduler-sdk/saashost"
	"github.com/domainry/domainry-scheduler-sdk/saashost/httptransport"
)

type Factory struct {
	transport  saashost.Transport
	httpConfig *httptransport.Config
}

func NewFactory(transport ...saashost.Transport) Factory {
	var selected saashost.Transport
	if len(transport) > 0 {
		selected = transport[0]
	}
	return Factory{transport: selected}
}

func NewHTTPFactory(config httptransport.Config) Factory { return Factory{httpConfig: &config} }

func (f Factory) Open(ctx context.Context, application schedulersdk.ApplicationRef) (schedulersdk.Binding, error) {
	return nil, fmt.Errorf("Scheduler SaaS requires Runtime host capabilities")
}

func (f Factory) OpenSaaS(ctx context.Context, application schedulersdk.ApplicationRef, host modulehost.Host) (schedulersdk.Binding, error) {
	if err := application.Validate(); err != nil {
		return nil, err
	}
	transport := f.transport
	var err error
	if transport == nil && f.httpConfig != nil {
		transport, err = httptransport.Open(ctx, *f.httpConfig)
		if err != nil {
			return nil, err
		}
	}
	if transport == nil {
		return nil, fmt.Errorf("Scheduler SaaS transport is required")
	}
	if host == nil || host.Definitions() == nil || host.Dispatcher() == nil {
		return nil, fmt.Errorf("Scheduler SaaS Runtime host is incomplete")
	}
	if binder, ok := transport.(saashost.ApplicationBindingTransport); ok {
		if err := binder.BindApplication(application); err != nil {
			return nil, fmt.Errorf("bind Scheduler SaaS transport application: %w", err)
		}
	}
	descriptor, err := transport.Descriptor(ctx, application)
	if err != nil {
		return nil, fmt.Errorf("read Scheduler SaaS descriptor: %w", err)
	}
	if err := descriptor.Validate(); err != nil {
		return nil, err
	}
	if descriptor.Mode != schedulersdk.DeploymentModeSaaS {
		return nil, fmt.Errorf("Scheduler remote descriptor mode must be saas")
	}
	if !descriptor.Supports(schedulersdk.CapabilityDefinitionPublicationFencing) {
		return nil, schedulersdk.ErrDefinitionPublicationCapabilityRequired
	}
	publication, ok := transport.(saashost.DefinitionPublicationTransport)
	if !ok {
		return nil, schedulersdk.ErrDefinitionPublicationCapabilityRequired
	}
	session, err := publication.BeginDefinitionPublisherSession(ctx, application)
	if err != nil {
		return nil, fmt.Errorf("begin Scheduler definition publisher session: %w", err)
	}
	if err := session.Validate(); err != nil {
		return nil, fmt.Errorf("begin Scheduler definition publisher session: %w", err)
	}
	return &binding{application: application, transport: transport, host: host, descriptor: descriptor, publication: session, closed: make(chan struct{})}, nil
}

type binding struct {
	application schedulersdk.ApplicationRef
	transport   saashost.Transport
	host        modulehost.Host
	descriptor  schedulersdk.Descriptor
	publication schedulersdk.DefinitionPublisherSession
	closed      chan struct{}
	closeOnce   sync.Once
}

func (b *binding) Descriptor() schedulersdk.Descriptor { return b.descriptor }
func (b *binding) CapabilitySummary(ctx context.Context) (modulecapability.ModuleSummary, error) {
	return b.transport.CapabilitySummary(ctx)
}
func (b *binding) CapabilityCategory(ctx context.Context, key string) (modulecapability.CategoryDocument, error) {
	return b.transport.CapabilityCategory(ctx, key)
}
func (b *binding) ValidateCapabilityCandidate(ctx context.Context, request modulecapability.ValidationRequest) (modulecapability.ValidationResult, error) {
	return b.transport.ValidateCapabilityCandidate(ctx, request)
}
func (b *binding) Reconcile(ctx context.Context) error {
	snapshot, err := b.host.Definitions().Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("read Runtime Scheduler definitions: %w", err)
	}
	// Runtime host snapshots carry process-local configuration only. The
	// Scheduler-issued session is the sole publication authority.
	snapshot.PublisherSession = nil
	if err := schedulersdk.ValidateModuleDefinitionSnapshot(snapshot); err != nil {
		return err
	}
	snapshot.PublisherSession = &b.publication
	return b.transport.Reconcile(ctx, b.application, snapshot)
}
func (b *binding) Preview(ctx context.Context, value schedulersdk.Schedule, after time.Time, count int) ([]time.Time, error) {
	return b.transport.Preview(ctx, b.application, value, after, count)
}
func (b *binding) Tick(ctx context.Context, now time.Time, limit int) (int, error) {
	return b.transport.Tick(ctx, b.application, now, limit)
}
func (b *binding) TriggerNow(ctx context.Context, key, reason string) (schedulersdk.Run, error) {
	return b.transport.TriggerNow(ctx, b.application, key, reason)
}
func (b *binding) Reschedule(ctx context.Context, key string, nextRunAt time.Time, reason string) error {
	return b.transport.Reschedule(ctx, b.application, key, nextRunAt, reason)
}
func (b *binding) Runs(ctx context.Context, limit int) ([]schedulersdk.Run, error) {
	return b.transport.Runs(ctx, b.application, limit)
}
func (b *binding) Run(ctx context.Context, id string) (schedulersdk.Run, error) {
	return b.transport.Run(ctx, b.application, id)
}
func (b *binding) RetryRun(ctx context.Context, id, reason string) (schedulersdk.Run, error) {
	return b.transport.RetryRun(ctx, b.application, id, reason)
}
func (b *binding) CancelRun(ctx context.Context, id, reason string) (schedulersdk.Run, error) {
	return b.transport.CancelRun(ctx, b.application, id, reason)
}
func (b *binding) DeadLetters(ctx context.Context, limit int) ([]schedulersdk.DeadLetter, error) {
	return b.transport.DeadLetters(ctx, b.application, limit)
}
func (b *binding) DeadLetter(ctx context.Context, id string) (schedulersdk.DeadLetter, error) {
	return b.transport.DeadLetter(ctx, b.application, id)
}
func (b *binding) ResolveDeadLetter(ctx context.Context, id, reason string) (schedulersdk.DeadLetter, error) {
	return b.transport.ResolveDeadLetter(ctx, b.application, id, reason)
}
func (b *binding) RequeueDeadLetter(ctx context.Context, id, reason string) (schedulersdk.Run, error) {
	return b.transport.RequeueDeadLetter(ctx, b.application, id, reason)
}
func (b *binding) Start(ctx context.Context, _ schedulersdk.WorkerConfig) <-chan struct{} {
	done := make(chan struct{})
	if ctx == nil {
		close(done)
		return done
	}
	select {
	case <-ctx.Done():
		close(done)
		return done
	case <-b.closed:
		close(done)
		return done
	default:
	}
	go func() {
		defer close(done)
		select {
		case <-ctx.Done():
		case <-b.closed:
		}
	}()
	return done // the SaaS control plane owns its clock worker
}

// Close releases only this Runtime-side handle. A stale publisher must never
// be able to stop the Scheduler-owned worker or delete application state.
func (b *binding) Close(context.Context) error {
	b.closeOnce.Do(func() { close(b.closed) })
	return nil
}

var _ schedulersdk.Factory = Factory{}
var _ saashost.Factory = Factory{}
var _ schedulersdk.Binding = (*binding)(nil)
