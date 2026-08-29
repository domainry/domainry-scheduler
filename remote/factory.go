package remote

import (
	"context"
	"fmt"
	"sync"
	"time"

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
		transport, err = httptransport.New(*f.httpConfig)
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
	return &binding{application: application, transport: transport, host: host, descriptor: descriptor}, nil
}

type binding struct {
	application schedulersdk.ApplicationRef
	transport   saashost.Transport
	host        modulehost.Host
	descriptor  schedulersdk.Descriptor
	startOnce   sync.Once
}

func (b *binding) Descriptor() schedulersdk.Descriptor { return b.descriptor }
func (b *binding) Reconcile(ctx context.Context) error {
	snapshot, err := b.host.Definitions().Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("read Runtime Scheduler definitions: %w", err)
	}
	for _, definition := range snapshot.Definitions {
		if err := definition.Validate(); err != nil {
			return err
		}
	}
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
