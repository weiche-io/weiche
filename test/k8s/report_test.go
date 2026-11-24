package k8s

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

// Each table is appended once, including rows collected before a failed subtest.
type metricTable struct{ strings.Builder }

func newMetricTable(title string, columns ...string) *metricTable {
	table := new(metricTable)
	fmt.Fprintf(table, "\n### %s · %s\n\n", title, os.Getenv("FAMILY"))
	fmt.Fprintf(table, "| %s |\n", strings.Join(columns, " | "))
	fmt.Fprintf(table, "|%s\n", strings.Repeat("---|", len(columns)))
	return table
}

func (table *metricTable) row(format string, args ...any) {
	fmt.Fprintf(table, "| "+format+" |\n", args...)
}

func (table *metricTable) publish(t *testing.T) {
	t.Helper()
	path := os.Getenv("GITHUB_STEP_SUMMARY")
	if path == "" {
		return
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer f.Close()
	if _, err := f.WriteString(table.String()); err != nil {
		t.Error(err)
	}
}
