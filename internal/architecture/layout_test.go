package architecture

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestRepositoryRootContainsOnlyReviewedPackages(t *testing.T) {
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve architecture test path")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
	allowed := map[string]bool{"admin": true, "internal": true, "module": true, "remote": true, "server": true}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !entry.IsDir() || strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		if !allowed[entry.Name()] {
			t.Errorf("unreviewed Scheduler root directory %q; implementation packages belong below internal", entry.Name())
		}
	}
}
