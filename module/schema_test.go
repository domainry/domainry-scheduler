package module

import (
	"slices"
	"testing"

	"github.com/domainry/domainry-foundation/schemaownership"
)

func TestModulePublishesSchedulerOwnedSchema(t *testing.T) {
	tables := SchemaOwnership()
	if err := schemaownership.ValidateAll(tables); err != nil {
		t.Fatal(err)
	}
	if len(tables) != 2 || !slices.Equal(OwnedTables(), schemaownership.Names(tables)) {
		t.Fatalf("Scheduler schema ownership=%d tables=%v", len(tables), OwnedTables())
	}
}
