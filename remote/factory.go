package remote

import (
	"context"
	"fmt"
	"sync"
	"time"

	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/domainry/domainry-scheduler-sdk/saashost"
)

type Factory struct{ transport saashost.Transport }

func NewFactory(transport ...saashost.Transport) Factory {
	var selected saashost.Transport
	if len(transport) > 0 {
		selected = transport[0]
	}
	return Factory{transport: selected}
}

func (f Factory) Open(ctx context.Context, application schedulersdk.ApplicationRef) (schedulersdk.Binding, error) {
	return f.OpenSaaS(ctx, application, f.transport)
}

func (Factory) OpenSaaS(ctx context.Context, application schedulersdk.ApplicationRef, transport saashost.Transport) (schedulersdk.Binding, error) {
	if err := application.Validate(); err != nil {
		return nil, err
	}
	if transport == nil {
		return nil, fmt.Errorf("Scheduler SaaS transport is required")
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
	return &binding{application: application, transport: transport, descriptor: descriptor}, nil
}

type binding struct {
	application schedulersdk.ApplicationRef
	transport   saashost.Transport
	descriptor  schedulersdk.Descriptor
	startOnce   sync.Once
}

func (b *binding) Descriptor() schedulersdk.Descriptor { return b.descriptor }
func (b *binding) Reconcile(ctx context.Context) error {
	return b.transport.Reconcile(ctx, b.application)
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
func (b *binding) Runs(ctx context.Context, limit int) ([]schedulersdk.Run, error) {
	return b.transport.Runs(ctx, b.application, limit)
}
func (b *binding) Start(ctx context.Context, config schedulersdk.WorkerConfig) <-chan struct{} {
	done := make(chan struct{})
	b.startOnce.Do(func() { close(done) })
	return done // the SaaS control plane owns its clock worker
}
func (b *binding) Close(ctx context.Context) error { return b.transport.Close(ctx, b.application) }

var _ schedulersdk.Factory = Factory{}
var _ saashost.Factory = Factory{}
var _ schedulersdk.Binding = (*binding)(nil)
