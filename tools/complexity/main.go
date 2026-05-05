// Command complexity reports per-function non-blank line count and a
// crude cyclomatic complexity score for active Go files.
//
// Usage:
//
//	go run ./tools/complexity                                # report all
//	go run ./tools/complexity -prod                          # production only (skips _test.go)
//	go run ./tools/complexity -max-lines 5 -max-cyclo 3      # fail if exceeded
//	go run ./tools/complexity -top 20                        # top 20 by lines+cyclo
//
// Excludes old/, vendor/, .git/, and the tool itself.
package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func main() {
	var (
		root     = flag.String("root", ".", "root directory to scan")
		prodOnly = flag.Bool("prod", false, "skip _test.go files")
		testOnly = flag.Bool("tests", false, "only _test.go files")
		maxLines = flag.Int("max-lines", 0, "fail if any function has more non-blank lines (0 = report only)")
		maxCyclo = flag.Int("max-cyclo", 0, "fail if any function has higher cyclomatic complexity (0 = report only)")
		top      = flag.Int("top", 0, "show only the worst N functions (0 = all)")
		summary  = flag.Bool("summary", false, "print only summary stats")
	)
	flag.Parse()

	report, err := scan(*root, *prodOnly, *testOnly)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	render(report, *maxLines, *maxCyclo, *top, *summary)
	if shouldFail(report, *maxLines, *maxCyclo) {
		os.Exit(1)
	}
}

// funcStat holds one function's metrics.
type funcStat struct {
	File  string
	Line  int
	Name  string
	Lines int
	Cyclo int
	Test  bool
}

// scan walks root, parses every .go file, and returns one funcStat per
// declared function/method.
func scan(root string, prodOnly, testOnly bool) ([]funcStat, error) {
	var out []funcStat
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if shouldSkipDir(d.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		isTest := strings.HasSuffix(path, "_test.go")
		if prodOnly && isTest {
			return nil
		}
		if testOnly && !isTest {
			return nil
		}
		stats, err := parseFile(fset, path, isTest)
		if err != nil {
			return err
		}
		out = append(out, stats...)
		return nil
	})
	return out, err
}

// shouldSkipDir filters traversal at the directory level.
func shouldSkipDir(name string) bool {
	switch name {
	case "old", "vendor", ".git", "site", "build", "dist":
		return true
	}
	return false
}

// parseFile returns one funcStat per function in path.
func parseFile(fset *token.FileSet, path string, isTest bool) ([]funcStat, error) {
	file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	var stats []funcStat
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		stats = append(stats, statFor(fset, path, fn, isTest))
	}
	return stats, nil
}

// statFor builds a funcStat from one FuncDecl.
func statFor(fset *token.FileSet, path string, fn *ast.FuncDecl, isTest bool) funcStat {
	pos := fset.Position(fn.Pos())
	return funcStat{
		File:  path,
		Line:  pos.Line,
		Name:  funcName(fn),
		Lines: nonBlankLines(fset, fn.Body),
		Cyclo: cyclomatic(fn.Body),
		Test:  isTest,
	}
}

// funcName produces "Recv.Name" for methods, "Name" for functions.
func funcName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}
	return recvTypeName(fn.Recv.List[0].Type) + "." + fn.Name.Name
}

// recvTypeName extracts the receiver type, stripping pointer wrapper.
func recvTypeName(expr ast.Expr) string {
	if star, ok := expr.(*ast.StarExpr); ok {
		expr = star.X
	}
	if id, ok := expr.(*ast.Ident); ok {
		return id.Name
	}
	return "?"
}

// nonBlankLines counts source lines inside body that contain at least
// one non-whitespace character. Cheap approximation; relies on the
// FileSet's line lookup rather than re-reading the file.
func nonBlankLines(fset *token.FileSet, body *ast.BlockStmt) int {
	if body == nil {
		return 0
	}
	startLine := fset.Position(body.Lbrace).Line
	endLine := fset.Position(body.Rbrace).Line
	if endLine <= startLine {
		return 0
	}
	data, err := os.ReadFile(fset.Position(body.Lbrace).Filename)
	if err != nil {
		return endLine - startLine - 1
	}
	return countNonBlank(data, startLine+1, endLine-1)
}

// countNonBlank counts how many lines in [first, last] (inclusive,
// 1-based) contain a non-whitespace rune.
func countNonBlank(src []byte, first, last int) int {
	if first > last {
		return 0
	}
	lines := strings.Split(string(src), "\n")
	if last > len(lines) {
		last = len(lines)
	}
	n := 0
	for i := first - 1; i < last; i++ {
		if strings.TrimSpace(lines[i]) != "" {
			n++
		}
	}
	return n
}

// cyclomatic walks body and adds one for each branching node:
// if, for, range, case, default, &&, ||, plus 1 baseline.
func cyclomatic(body ast.Node) int {
	c := 1
	ast.Inspect(body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.IfStmt, *ast.ForStmt, *ast.RangeStmt:
			c++
		case *ast.CaseClause, *ast.CommClause:
			c++
		case *ast.BinaryExpr:
			if x.Op == token.LAND || x.Op == token.LOR {
				c++
			}
		}
		return true
	})
	return c
}

// render prints the report.
func render(stats []funcStat, maxLines, maxCyclo, top int, summary bool) {
	sort.Slice(stats, func(i, j int) bool {
		// Worst (highest lines + cyclo) first.
		ai, aj := stats[i].Lines+stats[i].Cyclo, stats[j].Lines+stats[j].Cyclo
		if ai != aj {
			return ai > aj
		}
		if stats[i].File != stats[j].File {
			return stats[i].File < stats[j].File
		}
		return stats[i].Line < stats[j].Line
	})
	if !summary {
		printRows(stats, top)
	}
	printSummary(stats, maxLines, maxCyclo)
}

// printRows prints the per-function table; -top truncates.
func printRows(stats []funcStat, top int) {
	if top > 0 && top < len(stats) {
		stats = stats[:top]
	}
	fmt.Printf("%-6s %-6s %-50s %s\n", "LINES", "CYCLO", "FUNC", "LOC")
	for _, s := range stats {
		fmt.Printf("%-6d %-6d %-50s %s:%d\n",
			s.Lines, s.Cyclo, truncate(s.Name, 50), s.File, s.Line)
	}
}

// printSummary aggregates totals + threshold violations.
func printSummary(stats []funcStat, maxLines, maxCyclo int) {
	prod, tests := splitTests(stats)
	fmt.Println()
	fmt.Printf("Production: %s\n", aggregate(prod))
	fmt.Printf("Tests:      %s\n", aggregate(tests))
	if maxLines > 0 {
		fmt.Printf("Functions over max-lines=%d: %d\n", maxLines, countOver(stats, maxLines, 0))
	}
	if maxCyclo > 0 {
		fmt.Printf("Functions over max-cyclo=%d: %d\n", maxCyclo, countOver(stats, 0, maxCyclo))
	}
}

// splitTests partitions by Test flag.
func splitTests(stats []funcStat) (prod, tests []funcStat) {
	for _, s := range stats {
		if s.Test {
			tests = append(tests, s)
		} else {
			prod = append(prod, s)
		}
	}
	return prod, tests
}

// aggregate produces a one-line summary for a slice.
func aggregate(stats []funcStat) string {
	if len(stats) == 0 {
		return "0 functions"
	}
	o5, o10, o20, c10 := 0, 0, 0, 0
	for _, s := range stats {
		if s.Lines > 5 {
			o5++
		}
		if s.Lines > 10 {
			o10++
		}
		if s.Lines > 20 {
			o20++
		}
		if s.Cyclo >= 10 {
			c10++
		}
	}
	return fmt.Sprintf("%d funcs; >5L=%d  >10L=%d  >20L=%d  C>=10=%d",
		len(stats), o5, o10, o20, c10)
}

// countOver counts stats violating either threshold (zero ignored).
func countOver(stats []funcStat, maxLines, maxCyclo int) int {
	n := 0
	for _, s := range stats {
		if maxLines > 0 && s.Lines > maxLines {
			n++
			continue
		}
		if maxCyclo > 0 && s.Cyclo > maxCyclo {
			n++
		}
	}
	return n
}

// shouldFail returns true if either threshold is set and any
// production function exceeds it.
func shouldFail(stats []funcStat, maxLines, maxCyclo int) bool {
	if maxLines == 0 && maxCyclo == 0 {
		return false
	}
	for _, s := range stats {
		if s.Test {
			continue
		}
		if maxLines > 0 && s.Lines > maxLines {
			return true
		}
		if maxCyclo > 0 && s.Cyclo > maxCyclo {
			return true
		}
	}
	return false
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}
