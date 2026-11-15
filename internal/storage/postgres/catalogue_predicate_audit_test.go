package postgres_test

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/inexcession/tobiten/internal/storage"
	"github.com/inexcession/tobiten/internal/storage/postgres"
)

var predicateFragmentMethods = map[string]bool{
	"SeriesFragment":          true,
	"VolumeFragment":          true,
	"AssetFragment":           true,
	"ReadingProgressFragment": true,
}

type auditedFuncInfo struct {
	name            string
	receiverType    string
	exported        bool
	hasCtxAndScope  bool
	directCalls     []string
	fragmentCalls   []string
	callsLoadPred   bool
	callsDBQuery    bool
	auditViolations []string
}

type cataloguePredicateAuditReport struct {
	exportedMethods []string
	violations      []string
}

func TestCatalogueRepository_EveryReadMethodAppliesPredicate(t *testing.T) {
	// 1. Verify storage.CatalogueRepository interface and *postgres.CatalogueRepository match exactly.
	ifaceType := reflect.TypeOf((*storage.CatalogueRepository)(nil)).Elem()
	implType := reflect.TypeOf((*postgres.CatalogueRepository)(nil))

	ifaceMethods := make([]string, 0, ifaceType.NumMethod())
	for i := range ifaceType.NumMethod() {
		m := ifaceType.Method(i)
		ifaceMethods = append(ifaceMethods, m.Name)
		assertMethodHasCtxAndScope(t, "storage.CatalogueRepository."+m.Name, m.Type, false)
	}
	slices.Sort(ifaceMethods)

	implMethods := make([]string, 0, implType.NumMethod())
	for i := range implType.NumMethod() {
		m := implType.Method(i)
		implMethods = append(implMethods, m.Name)
		assertMethodHasCtxAndScope(t, "*postgres.CatalogueRepository."+m.Name, m.Type, true)
	}
	slices.Sort(implMethods)

	if !slices.Equal(ifaceMethods, implMethods) {
		t.Fatalf("interface and implementation exported methods diverged:\niface: %v\nimpl:  %v", ifaceMethods, implMethods)
	}

	// 2. Verify coverage map parity: every read method must have a corresponding enforcement test spec.
	if err := checkEnforcementCoverageParity(implMethods, catalogueReadEnforcementSpecs); err != nil {
		t.Fatalf("enforcement test coverage parity failed: %v", err)
	}

	// 3. Parse actual postgres package source and audit every exported CatalogueRepository method.
	report, err := auditCatalogueRepositoryFiles([]string{
		filepath.Join(".", "catalogue_repository.go"),
	}, nil)
	if err != nil {
		t.Fatalf("auditing catalogue_repository.go: %v", err)
	}

	// Also check no other non-test .go files in internal/storage/postgres declare methods on CatalogueRepository.
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading package directory: %v", err)
	}
	var pkgFiles []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") || strings.HasSuffix(name, ".sql.go") {
			continue
		}
		pkgFiles = append(pkgFiles, filepath.Join(".", name))
	}
	fullReport, err := auditCatalogueRepositoryFiles(pkgFiles, nil)
	if err != nil {
		t.Fatalf("auditing postgres package files: %v", err)
	}

	slices.Sort(report.exportedMethods)
	slices.Sort(fullReport.exportedMethods)
	if !slices.Equal(fullReport.exportedMethods, implMethods) {
		t.Fatalf("AST exported methods %v != reflect methods %v", fullReport.exportedMethods, implMethods)
	}
	if len(fullReport.violations) > 0 {
		t.Fatalf("catalogue repository predicate audit violations:\n- %s", strings.Join(fullReport.violations, "\n- "))
	}
}

func TestCatalogueRepository_PredicateAuditDetectsMissingPredicate(t *testing.T) {
	baseSrc, err := os.ReadFile("catalogue_repository.go")
	if err != nil {
		t.Fatalf("reading catalogue_repository.go: %v", err)
	}

	t.Run("rejects new read method missing CatalogueReadScope", func(t *testing.T) {
		mutated := string(baseSrc) + `
func (r *CatalogueRepository) GetSeriesUnsafe(ctx context.Context, id string) (*domain.Series, error) {
	return scanSeriesRow(r.db.QueryRow(ctx, selectSeriesColumns+" WHERE s.id = $1", id))
}
`
		report, err := auditCatalogueRepositoryFiles(nil, map[string]string{"catalogue_repository.go": mutated})
		if err != nil {
			t.Fatalf("audit error: %v", err)
		}
		if len(report.violations) == 0 {
			t.Fatal("expected audit violation for GetSeriesUnsafe without scope or predicate")
		}
	})

	t.Run("rejects new read method with scope that skips LabelPredicate", func(t *testing.T) {
		mutated := string(baseSrc) + `
func (r *CatalogueRepository) ListSeriesBypass(ctx context.Context, scope storage.CatalogueReadScope) ([]domain.Series, error) {
	_, _ = r.db.Query(ctx, selectSeriesColumns)
	return nil, nil
}
`
		report, err := auditCatalogueRepositoryFiles(nil, map[string]string{"catalogue_repository.go": mutated})
		if err != nil {
			t.Fatalf("audit error: %v", err)
		}
		if len(report.violations) == 0 {
			t.Fatal("expected audit violation for ListSeriesBypass skipping LabelPredicate")
		}
	})

	t.Run("rejects new read method calling loadPredicate without fragment", func(t *testing.T) {
		mutated := string(baseSrc) + `
func (r *CatalogueRepository) CountSeriesNoFragment(ctx context.Context, scope storage.CatalogueReadScope) (int64, error) {
	if _, err := r.loadPredicate(ctx, scope); err != nil {
		return 0, err
	}
	return r.queryCountWithSQL(ctx, "SELECT COUNT(*) FROM series s")
}
`
		report, err := auditCatalogueRepositoryFiles(nil, map[string]string{"catalogue_repository.go": mutated})
		if err != nil {
			t.Fatalf("audit error: %v", err)
		}
		if len(report.violations) == 0 {
			t.Fatal("expected audit violation for CountSeriesNoFragment without fragment call")
		}
	})

	t.Run("rejects new read method discarding fragment error", func(t *testing.T) {
		mutated := string(baseSrc) + `
func (r *CatalogueRepository) CountSeriesDiscardedErr(ctx context.Context, scope storage.CatalogueReadScope) (int64, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return 0, err
	}
	frag, _ := pred.SeriesFragment("s", 1)
	return r.queryCountWithSQL(ctx, "SELECT COUNT(*) FROM series s WHERE "+frag.SQL, frag.Args...)
}
`
		report, err := auditCatalogueRepositoryFiles(nil, map[string]string{"catalogue_repository.go": mutated})
		if err != nil {
			t.Fatalf("audit error: %v", err)
		}
		if len(report.violations) == 0 {
			t.Fatal("expected audit violation for CountSeriesDiscardedErr ignoring fragment error")
		}
	})

	t.Run("rejects new read method ignoring SQLFragment SQL or Args", func(t *testing.T) {
		mutated := string(baseSrc) + `
func (r *CatalogueRepository) CountSeriesUnusedFragment(ctx context.Context, scope storage.CatalogueReadScope) (int64, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return 0, err
	}
	frag, err := pred.SeriesFragment("s", 1)
	if err != nil {
		return 0, err
	}
	_ = frag
	return r.queryCountWithSQL(ctx, "SELECT COUNT(*) FROM series s")
}
`
		report, err := auditCatalogueRepositoryFiles(nil, map[string]string{"catalogue_repository.go": mutated})
		if err != nil {
			t.Fatalf("audit error: %v", err)
		}
		if len(report.violations) == 0 {
			t.Fatal("expected audit violation for CountSeriesUnusedFragment not wiring frag.SQL/frag.Args")
		}
	})

	t.Run("rejects new read method missing from enforcement test map", func(t *testing.T) {
		withExtra := append(slices.Clone([]string{"GetSeriesByID", "ListSeries"}), "CountUnwiredBooks")
		if err := checkEnforcementCoverageParity(withExtra, catalogueReadEnforcementSpecs); err == nil {
			t.Fatal("expected coverage parity error when new read method has no enforcement test spec")
		}
	})
}

func assertMethodHasCtxAndScope(t *testing.T, methodName string, fnType reflect.Type, hasReceiver bool) {
	t.Helper()
	offset := 0
	if hasReceiver {
		offset = 1
	}
	if fnType.NumIn() < offset+2 {
		t.Fatalf("%s must accept (ctx context.Context, scope storage.CatalogueReadScope, ...), got %d params", methodName, fnType.NumIn()-offset)
	}
	ctxType := reflect.TypeOf((*context.Context)(nil)).Elem()
	scopeType := reflect.TypeOf(storage.CatalogueReadScope{})
	if fnType.In(offset) != ctxType {
		t.Fatalf("%s param 0 must be context.Context, got %s", methodName, fnType.In(offset))
	}
	if fnType.In(offset+1) != scopeType {
		t.Fatalf("%s param 1 must be storage.CatalogueReadScope, got %s", methodName, fnType.In(offset+1))
	}
}

func checkEnforcementCoverageParity(methods []string, specs map[string]catalogueReadEnforcementSpec) error {
	var missing []string
	methodSet := make(map[string]bool, len(methods))
	for _, m := range methods {
		methodSet[m] = true
		if _, ok := specs[m]; !ok {
			missing = append(missing, m)
		}
	}
	var extra []string
	for k := range specs {
		if !methodSet[k] {
			extra = append(extra, k)
		}
	}
	if len(missing) > 0 || len(extra) > 0 {
		return fmt.Errorf("missing enforcement specs=%v, stale specs=%v", missing, extra)
	}
	return nil
}

func auditCatalogueRepositoryFiles(paths []string, inline map[string]string) (*cataloguePredicateAuditReport, error) {
	fset := token.NewFileSet()
	funcs := make(map[string]*auditedFuncInfo)

	parseOne := func(filename string, src any) error {
		fileAST, err := parser.ParseFile(fset, filename, src, 0)
		if err != nil {
			return fmt.Errorf("parsing %s: %w", filename, err)
		}
		for _, decl := range fileAST.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			info := inspectAuditedFunc(fd)
			key := funcKey(info.receiverType, info.name)
			funcs[key] = info
		}
		return nil
	}

	for _, p := range paths {
		if err := parseOne(p, nil); err != nil {
			return nil, err
		}
	}
	for name, src := range inline {
		if err := parseOne(name, src); err != nil {
			return nil, err
		}
	}

	report := &cataloguePredicateAuditReport{}
	for _, info := range funcs {
		if info.receiverType != "CatalogueRepository" || !info.exported {
			continue
		}
		report.exportedMethods = append(report.exportedMethods, info.name)
		violations := verifyExportedMethodPredicate(info.name, funcs)
		report.violations = append(report.violations, violations...)
	}
	slices.Sort(report.exportedMethods)
	slices.Sort(report.violations)
	return report, nil
}

func funcKey(receiverType, name string) string {
	if receiverType == "" {
		return name
	}
	return receiverType + "." + name
}

func receiverTypeName(fd *ast.FuncDecl) string {
	if fd.Recv == nil || len(fd.Recv.List) == 0 {
		return ""
	}
	expr := fd.Recv.List[0].Type
	for {
		switch t := expr.(type) {
		case *ast.StarExpr:
			expr = t.X
		case *ast.IndexExpr:
			expr = t.X
		case *ast.IndexListExpr:
			expr = t.X
		case *ast.Ident:
			return t.Name
		default:
			return ""
		}
	}
}

func hasASTCtxAndScopeParams(fd *ast.FuncDecl) bool {
	if fd.Type.Params == nil {
		return false
	}
	var paramTypes []ast.Expr
	for _, field := range fd.Type.Params.List {
		n := len(field.Names)
		if n == 0 {
			n = 1
		}
		for range n {
			paramTypes = append(paramTypes, field.Type)
		}
	}
	if len(paramTypes) < 2 {
		return false
	}
	return isSelectorType(paramTypes[0], "context", "Context") &&
		isSelectorType(paramTypes[1], "storage", "CatalogueReadScope")
}

func isSelectorType(expr ast.Expr, pkg, name string) bool {
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	ident, ok := sel.X.(*ast.Ident)
	return ok && ident.Name == pkg && sel.Sel.Name == name
}

func inspectAuditedFunc(fd *ast.FuncDecl) *auditedFuncInfo {
	info := &auditedFuncInfo{
		name:           fd.Name.Name,
		receiverType:   receiverTypeName(fd),
		exported:       ast.IsExported(fd.Name.Name),
		hasCtxAndScope: hasASTCtxAndScopeParams(fd),
	}

	fragVars := make(map[string]bool)
	usedSQL := make(map[string]bool)
	usedArgs := make(map[string]bool)

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.CallExpr:
			recordCallReferences(node, info)
		case *ast.AssignStmt:
			inspectPredicateAssignment(node, info, fragVars)
		case *ast.ExprStmt:
			if call, ok := node.X.(*ast.CallExpr); ok {
				if name := callTargetName(call.Fun); isPredicateOrFragTarget(name) {
					info.auditViolations = append(info.auditViolations,
						fmt.Sprintf("%s ignores return values of %s", info.name, name))
				}
			}
		case *ast.SelectorExpr:
			if ident, ok := node.X.(*ast.Ident); ok && fragVars[ident.Name] {
				if node.Sel.Name == "SQL" {
					usedSQL[ident.Name] = true
				}
				if node.Sel.Name == "Args" {
					usedArgs[ident.Name] = true
				}
			}
		}
		return true
	})

	for v := range fragVars {
		if !usedSQL[v] || !usedArgs[v] {
			info.auditViolations = append(info.auditViolations,
				fmt.Sprintf("%s creates SQLFragment %q without wiring both .SQL and .Args", info.name, v))
		}
	}

	return info
}

func recordCallReferences(call *ast.CallExpr, info *auditedFuncInfo) {
	recordTarget := func(target string) {
		if target == "" {
			return
		}
		if target == "LoadLabelPredicate" {
			info.callsLoadPred = true
		}
		if predicateFragmentMethods[target] {
			info.fragmentCalls = append(info.fragmentCalls, target)
		}
		if target == "Query" || target == "QueryRow" {
			info.callsDBQuery = true
		}
		info.directCalls = append(info.directCalls, target)
	}

	recordTarget(callTargetName(call.Fun))
	for _, arg := range call.Args {
		if target := exprTargetName(arg); target != "" {
			recordTarget(target)
		}
	}
}

func inspectPredicateAssignment(assign *ast.AssignStmt, info *auditedFuncInfo, fragVars map[string]bool) {
	if len(assign.Rhs) != 1 {
		return
	}
	call, ok := assign.Rhs[0].(*ast.CallExpr)
	if !ok {
		return
	}
	target := callTargetName(call.Fun)
	if !isPredicateOrFragTarget(target) {
		return
	}
	if len(assign.Lhs) < 2 {
		info.auditViolations = append(info.auditViolations,
			fmt.Sprintf("%s does not capture error from %s", info.name, target))
		return
	}
	if errIdent, ok := assign.Lhs[len(assign.Lhs)-1].(*ast.Ident); ok && errIdent.Name == "_" {
		info.auditViolations = append(info.auditViolations,
			fmt.Sprintf("%s discards error from %s using _", info.name, target))
	}
	if predicateFragmentMethods[target] || target == "fragmentFn" {
		if fragIdent, ok := assign.Lhs[0].(*ast.Ident); ok {
			if fragIdent.Name == "_" {
				info.auditViolations = append(info.auditViolations,
					fmt.Sprintf("%s discards SQLFragment from %s using _", info.name, target))
			} else {
				fragVars[fragIdent.Name] = true
			}
		}
	}
}

func isPredicateOrFragTarget(name string) bool {
	return name == "LoadLabelPredicate" || name == "loadPredicate" || name == "fragmentFn" || predicateFragmentMethods[name]
}

func callTargetName(fun ast.Expr) string {
	switch e := fun.(type) {
	case *ast.Ident:
		return e.Name
	case *ast.SelectorExpr:
		return e.Sel.Name
	case *ast.IndexExpr:
		return callTargetName(e.X)
	case *ast.IndexListExpr:
		return callTargetName(e.X)
	default:
		return ""
	}
}

func exprTargetName(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.Ident:
		return e.Name
	case *ast.SelectorExpr:
		return e.Sel.Name
	default:
		return ""
	}
}

func verifyExportedMethodPredicate(methodName string, funcs map[string]*auditedFuncInfo) []string {
	root := funcs[funcKey("CatalogueRepository", methodName)]
	if root == nil {
		return []string{fmt.Sprintf("method %s not found", methodName)}
	}
	var violations []string
	if !root.hasCtxAndScope {
		violations = append(violations, fmt.Sprintf("%s must take (context.Context, storage.CatalogueReadScope) as its first two parameters", methodName))
	}

	visited := make(map[string]bool)
	queue := []string{funcKey("CatalogueRepository", methodName)}
	sawLoadPred := false
	sawFragment := false
	sawDBQuery := false

	for len(queue) > 0 {
		currKey := queue[0]
		queue = queue[1:]
		if visited[currKey] {
			continue
		}
		visited[currKey] = true

		info := funcs[currKey]
		if info == nil {
			continue
		}
		if info.callsLoadPred {
			sawLoadPred = true
		}
		if len(info.fragmentCalls) > 0 {
			sawFragment = true
		}
		if info.callsDBQuery {
			sawDBQuery = true
		}
		violations = append(violations, info.auditViolations...)

		for _, callee := range info.directCalls {
			if repoKey := funcKey("CatalogueRepository", callee); funcs[repoKey] != nil && !visited[repoKey] {
				queue = append(queue, repoKey)
			}
			if pkgKey := funcKey("", callee); funcs[pkgKey] != nil && !visited[pkgKey] {
				queue = append(queue, pkgKey)
			}
		}
	}

	if !sawLoadPred {
		violations = append(violations, fmt.Sprintf("%s does not transitively call LoadLabelPredicate", methodName))
	}
	if !sawFragment {
		violations = append(violations, fmt.Sprintf("%s does not transitively call any LabelPredicate fragment builder", methodName))
	}
	if !sawDBQuery {
		violations = append(violations, fmt.Sprintf("%s does not transitively execute a database query", methodName))
	}
	return violations
}
