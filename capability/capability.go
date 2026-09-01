// Package capability exposes Scheduler's source-owned capability contract
// without opening persistence, workers, or HTTP executors.
package capability

import (
	"github.com/domainry/domainry-foundation/modulecapability"
	internalcapability "github.com/domainry/domainry-scheduler/internal/capability"
)

type Inputs struct{}

func Open(Inputs) (*modulecapability.StaticBinding, error) {
	return internalcapability.NewBinding()
}
