// Package capability exposes Scheduler's source-owned capability contract
// without opening persistence, workers, or HTTP executors.
package capability

import (
	"github.com/domainry/domainry-foundation/modulecapability"
)

type Inputs struct{}

func Open(inputs Inputs) (*modulecapability.StaticBinding, error) {
	return openContract(inputs)
}
