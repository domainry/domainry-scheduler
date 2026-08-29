// Package persistence is the Scheduler database composition boundary.
// Concrete repositories live below database/ so hosts never depend on an
// implementation package and repositories never select database engines.
package persistence

import (
	schedulermodulehost "github.com/domainry/domainry-scheduler-sdk/modulehost"
	schedulestore "github.com/domainry/domainry-scheduler/internal/infrastructure/persistence/database/schedule"
)

type Store = schedulestore.Store

func NewStore(database schedulermodulehost.Database, dialect schedulermodulehost.Dialect, runtimeID, workerID string) (*Store, error) {
	return schedulestore.New(database, dialect, runtimeID, workerID)
}
