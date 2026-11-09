// Package report 把场景结果渲染成 Markdown。
package report

import (
	"fmt"
	"strings"
	"time"
)

type Table struct {
	Headers []string
	Rows    [][]string
}

type Section struct {
	Title string
	Body  string
	Table *Table
	Notes []string
}

type Report struct {
	Title     string
	Generated time.Time
	Sections  []Section
}

func (r *Report) Add(s Section) { r.Sections = append(r.Sections, s) }

func (r *Report) Markdown() string {
	var b strings.Builder
	fmt.Fprintf(&b, "# %s\n\n", r.Title)
	fmt.Fprintf(&b, "_generated %s_\n\n", r.Generated.Format("2006-01-02 15:04:05"))

	for _, s := range r.Sections {
		fmt.Fprintf(&b, "## %s\n\n", s.Title)
		if s.Body != "" {
			fmt.Fprintf(&b, "%s\n\n", s.Body)
		}
		if s.Table != nil && len(s.Table.Rows) > 0 {
			b.WriteString(renderTable(s.Table))
			b.WriteString("\n")
		}
		for _, n := range s.Notes {
			fmt.Fprintf(&b, "- %s\n", n)
		}
		if len(s.Notes) > 0 {
			b.WriteString("\n")
		}
	}
	return b.String()
}

func renderTable(t *Table) string {
	var b strings.Builder
	b.WriteString("| " + strings.Join(t.Headers, " | ") + " |\n")
	sep := make([]string, len(t.Headers))
	for i := range sep {
		sep[i] = "---"
	}
	b.WriteString("| " + strings.Join(sep, " | ") + " |\n")
	for _, row := range t.Rows {
		b.WriteString("| " + strings.Join(row, " | ") + " |\n")
	}
	return b.String()
}

func Dur(d time.Duration) string {
	switch {
	case d == 0:
		return "0"
	case d < time.Microsecond:
		return fmt.Sprintf("%dns", d.Nanoseconds())
	case d < time.Millisecond:
		return fmt.Sprintf("%.1fµs", float64(d)/float64(time.Microsecond))
	case d < time.Second:
		return fmt.Sprintf("%.2fms", float64(d)/float64(time.Millisecond))
	default:
		return fmt.Sprintf("%.2fs", d.Seconds())
	}
}

// Check 渲染一行带 PASS/FAIL 的断言。
func Check(ok bool, description string) string {
	if ok {
		return "PASS  " + description
	}
	return "FAIL  " + description
}
