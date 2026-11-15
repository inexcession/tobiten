package postgres_test

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
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

var dbExecutionMethods = map[string]bool{
	"Query":     true,
	"QueryRow":  true,
	"Exec":      true,
	"QueryFunc": true,
	"SendBatch": true,
	"CopyFrom":  true,
	"Begin":     true,
	"BeginTx":   true,
}

var guardedBoundaryQueries = map[string]bool{
	"guardedCatalogueDB.query":    true,
	"guardedCatalogueDB.queryRow": true,
}

type auditedFuncInfo struct {
	key             string
	name            string
	filename        string
	receiverType    string
	receiverIdent   string
	paramTypes      map[string]string
	exported        bool
	hasCtxAndScope  bool
	decl            *ast.FuncDecl
	directCalls     []string
	callsLoadPred   bool
	rawDBCalls      []string
	callsSQLCNew    bool
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

	// 3. Parse all non-test package files and run the structural boundary check.
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

	assertRejected := func(t *testing.T, mutated, wantSubstr, desc string) {
		t.Helper()
		report, err := auditCatalogueRepositoryFiles(nil, map[string]string{"catalogue_repository.go": mutated})
		if err != nil {
			t.Fatalf("audit error: %v", err)
		}
		if len(report.violations) == 0 {
			t.Fatalf("expected audit violation for %s", desc)
		}
		for _, v := range report.violations {
			if strings.Contains(v, wantSubstr) {
				return
			}
		}
		t.Fatalf("expected violation containing %q for %s, got: %v", wantSubstr, desc, report.violations)
	}

	t.Run("rejects new read method missing CatalogueReadScope", func(t *testing.T) {
		mutated := string(baseSrc) + `
func (r *CatalogueRepository) GetSeriesUnsafe(ctx context.Context, id string) (*domain.Series, error) {
	return scanSeriesRow(r.guard.db.QueryRow(ctx, selectSeriesColumns+" WHERE s.id = $1", id))
}
`
		assertRejected(t, mutated, "GetSeriesUnsafe", "GetSeriesUnsafe without scope or predicate")
	})

	t.Run("rejects new read method skipping LabelPredicate", func(t *testing.T) {
		mutated := string(baseSrc) + `
func (r *CatalogueRepository) ListSeriesBypass(ctx context.Context, scope storage.CatalogueReadScope) ([]domain.Series, error) {
	_, _ = r.guard.query(ctx, nil, guardedQuerySpec{
		selectFrom: selectSeriesColumns,
		predicates: []predicateClause{{alias: "s", fragmentFn: (*LabelPredicate).SeriesFragment}},
	})
	return nil, nil
}
`
		assertRejected(t, mutated, "ListSeriesBypass", "ListSeriesBypass skipping loadPredicate")
	})

	t.Run("rejects new read method discarding loadPredicate error", func(t *testing.T) {
		mutated := string(baseSrc) + `
func (r *CatalogueRepository) CountSeriesDiscardedErr(ctx context.Context, scope storage.CatalogueReadScope) (int64, error) {
	pred, _ := r.loadPredicate(ctx, scope)
	return queryCount(ctx, &r.guard, pred, guardedQuerySpec{
		selectFrom: "SELECT COUNT(*) FROM series s",
		predicates: []predicateClause{{alias: "s", fragmentFn: (*LabelPredicate).SeriesFragment}},
	})
}
`
		assertRejected(t, mutated, "CountSeriesDiscardedErr", "CountSeriesDiscardedErr ignoring loadPredicate error")
	})

	t.Run("rejects raw DB query inside otherwise guarded method", func(t *testing.T) {
		mutated := string(baseSrc) + `
func (r *CatalogueRepository) GetSeriesWithRawQuery(ctx context.Context, scope storage.CatalogueReadScope, id string) (*domain.Series, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return nil, err
	}
	var extra string
	_ = r.guard.db.QueryRow(ctx, "SELECT title FROM series WHERE id = $1", id).Scan(&extra)
	return queryOne(ctx, &r.guard, pred, guardedQuerySpec{
		selectFrom: selectSeriesColumns,
		whereParts: []string{"s.id = $1"},
		args:       []any{id},
		predicates: []predicateClause{{alias: "s", fragmentFn: (*LabelPredicate).SeriesFragment}},
	}, scanSeriesRow)
}
`
		assertRejected(t, mutated, "executes raw DB method QueryRow outside guardedCatalogueDB", "raw QueryRow inside guarded method")
	})

	t.Run("rejects raw DB query inside helper called from guarded method", func(t *testing.T) {
		mutated := string(baseSrc) + `
func (r *CatalogueRepository) GetVolumeWithRawHelper(ctx context.Context, scope storage.CatalogueReadScope, id string) (*domain.Volume, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return nil, err
	}
	vol, err := r.getVolumeWithPredicate(ctx, pred, id)
	if err != nil {
		return nil, err
	}
	if err := r.rawLoadMembershipsHelper(ctx, vol.ID); err != nil {
		return nil, err
	}
	return vol, nil
}

func (r *CatalogueRepository) rawLoadMembershipsHelper(ctx context.Context, volumeID string) error {
	rows, err := r.guard.db.Query(ctx, "SELECT series_id FROM series_memberships WHERE volume_id = $1", volumeID)
	if err != nil {
		return err
	}
	rows.Close()
	return nil
}
`
		assertRejected(t, mutated, "rawLoadMembershipsHelper executes raw DB method Query outside guardedCatalogueDB", "raw Query in helper reachable from guarded method")
	})

	t.Run("rejects direct sqlc Queries bypass inside otherwise guarded method", func(t *testing.T) {
		mutated := string(baseSrc) + `
func (r *CatalogueRepository) GetSeriesWithSQLCBypass(ctx context.Context, scope storage.CatalogueReadScope, id string) (*domain.Series, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return nil, err
	}
	_, _ = New(r.guard.db).GetUserByID(ctx, "u-1")
	return queryOne(ctx, &r.guard, pred, guardedQuerySpec{
		selectFrom: selectSeriesColumns,
		whereParts: []string{"s.id = $1"},
		args:       []any{id},
		predicates: []predicateClause{{alias: "s", fragmentFn: (*LabelPredicate).SeriesFragment}},
	}, scanSeriesRow)
}
`
		assertRejected(t, mutated, "GetSeriesWithSQLCBypass invokes sqlc New()", "direct sqlc New() bypass inside guarded method")
	})

	t.Run("rejects guardedQuerySpec missing predicates", func(t *testing.T) {
		mutated := string(baseSrc) + `
func (r *CatalogueRepository) ListSeriesEmptyPredicates(ctx context.Context, scope storage.CatalogueReadScope) ([]domain.Series, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return nil, err
	}
	return queryList(ctx, &r.guard, pred, guardedQuerySpec{
		selectFrom: selectSeriesColumns,
	}, scanSeriesRow)
}
`
		assertRejected(t, mutated, "guardedQuerySpec missing predicates", "guardedQuerySpec omitting predicates")
	})

	t.Run("rejects guardedQuerySpec with invalid predicate clause", func(t *testing.T) {
		mutated := string(baseSrc) + `
func (r *CatalogueRepository) ListSeriesNilFragmentFn(ctx context.Context, scope storage.CatalogueReadScope) ([]domain.Series, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return nil, err
	}
	return queryList(ctx, &r.guard, pred, guardedQuerySpec{
		selectFrom: selectSeriesColumns,
		predicates: []predicateClause{{alias: "s"}},
	}, scanSeriesRow)
}
`
		assertRejected(t, mutated, "invalid predicateClause", "predicateClause without valid fragment builder")
	})

	t.Run("rejects guardedCatalogueDB queryRow bypassing prepare", func(t *testing.T) {
		mutated := strings.Replace(
			string(baseSrc),
			"func (g *guardedCatalogueDB) queryRow(ctx context.Context, pred *LabelPredicate, spec guardedQuerySpec) (pgx.Row, error) {\n\tquery, args, err := g.prepare(pred, spec)\n\tif err != nil {\n\t\treturn nil, err\n\t}\n\treturn g.db.QueryRow(ctx, query, args...), nil\n}",
			"func (g *guardedCatalogueDB) queryRow(ctx context.Context, pred *LabelPredicate, spec guardedQuerySpec) (pgx.Row, error) {\n\treturn g.db.QueryRow(ctx, spec.selectFrom), nil\n}",
			1,
		)
		assertRejected(t, mutated, "guardedCatalogueDB.queryRow", "guardedCatalogueDB.queryRow skipping prepare")
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
	var structuralViolations []string
	sawRepoStruct := false

	parseOne := func(filename string, src any) error {
		fileAST, err := parser.ParseFile(fset, filename, src, 0)
		if err != nil {
			return fmt.Errorf("parsing %s: %w", filename, err)
		}
		base := filepath.Base(filename)
		for _, decl := range fileAST.Decls {
			switch d := decl.(type) {
			case *ast.GenDecl:
				if found, v := checkCatalogueRepositoryStruct(d); found {
					sawRepoStruct = true
					structuralViolations = append(structuralViolations, v...)
				}
			case *ast.FuncDecl:
				if d.Body == nil {
					continue
				}
				recv, recvIdent := receiverTypeAndIdent(d)
				key := funcKey(recv, d.Name.Name)
				info := &auditedFuncInfo{
					key:            key,
					name:           d.Name.Name,
					filename:       base,
					receiverType:   recv,
					receiverIdent:  recvIdent,
					paramTypes:     collectParamBaseTypes(d),
					exported:       ast.IsExported(d.Name.Name),
					hasCtxAndScope: hasASTCtxAndScopeParams(d),
					decl:           d,
				}
				inspectFuncStructure(info)
				funcs[key] = info
			}
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
	if !sawRepoStruct {
		structuralViolations = append(structuralViolations, "CatalogueRepository struct declaration not found")
	}
	structuralViolations = append(structuralViolations, checkGuardedBoundaryImplementation(funcs)...)

	// Audit every function in catalogue_repository.go and every method on CatalogueRepository / guardedCatalogueDB
	// across the package so no parallel unguarded query helper can hide in another file.
	for _, info := range funcs {
		if info.filename != "catalogue_repository.go" &&
			info.receiverType != "CatalogueRepository" &&
			info.receiverType != "guardedCatalogueDB" {
			continue
		}
		structuralViolations = append(structuralViolations, checkRawAccess(info)...)
	}

	for _, info := range funcs {
		if info.receiverType != "CatalogueRepository" || !info.exported {
			continue
		}
		report.exportedMethods = append(report.exportedMethods, info.name)
		structuralViolations = append(structuralViolations, verifyExportedMethodUsesBoundary(info, funcs)...)
	}

	seen := make(map[string]bool, len(structuralViolations))
	for _, v := range structuralViolations {
		if !seen[v] {
			seen[v] = true
			report.violations = append(report.violations, v)
		}
	}
	slices.Sort(report.exportedMethods)
	slices.Sort(report.violations)
	return report, nil
}

func checkCatalogueRepositoryStruct(gd *ast.GenDecl) (bool, []string) {
	if gd.Tok != token.TYPE {
		return false, nil
	}
	for _, spec := range gd.Specs {
		ts, ok := spec.(*ast.TypeSpec)
		if !ok || ts.Name.Name != "CatalogueRepository" {
			continue
		}
		st, ok := ts.Type.(*ast.StructType)
		if !ok || st.Fields == nil || len(st.Fields.List) != 1 {
			return true, []string{"CatalogueRepository must contain only the guard guardedCatalogueDB field"}
		}
		field := st.Fields.List[0]
		id, ok := field.Type.(*ast.Ident)
		if len(field.Names) != 1 || field.Names[0].Name != "guard" || !ok || id.Name != "guardedCatalogueDB" {
			return true, []string{"CatalogueRepository must hold guard guardedCatalogueDB instead of raw DB handles"}
		}
		return true, nil
	}
	return false, nil
}

func checkGuardedBoundaryImplementation(funcs map[string]*auditedFuncInfo) []string {
	var violations []string

	loadPred := funcs["guardedCatalogueDB.loadPredicate"]
	if loadPred == nil {
		violations = append(violations, "guardedCatalogueDB.loadPredicate is missing")
	} else if !slices.Contains(loadPred.directCalls, "LoadLabelPredicate") {
		violations = append(violations, "guardedCatalogueDB.loadPredicate must delegate to LoadLabelPredicate")
	}

	prep := funcs["guardedCatalogueDB.prepare"]
	if prep == nil {
		violations = append(violations, "guardedCatalogueDB.prepare is missing")
	} else {
		violations = append(violations, verifyPrepareEnforcesPredicates(prep)...)
	}

	for _, fnName := range []string{"guardedCatalogueDB.query", "guardedCatalogueDB.queryRow"} {
		fn := funcs[fnName]
		if fn == nil {
			violations = append(violations, fnName+" is missing")
			continue
		}
		if !verifyBoundaryExecCallsPrepareFirst(fn) {
			violations = append(violations, fmt.Sprintf("%s must validate via g.prepare(pred, spec) before executing DB query", fnName))
		}
	}
	return violations
}

func verifyPrepareEnforcesPredicates(info *auditedFuncInfo) []string {
	var (
		hasEmptyPredCheck bool
		hasFragCall       bool
		hasFragErrCheck   bool
		usesFragSQL       bool
		usesFragArgs      bool
	)

	ast.Inspect(info.decl.Body, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.IfStmt:
			if hasLenCallOn(node.Cond, "spec", "predicates") {
				hasEmptyPredCheck = true
			}
		case *ast.AssignStmt:
			if len(node.Rhs) == 1 && isCallToSelector(node.Rhs[0], "fragmentFn") {
				hasFragCall = true
				if len(node.Lhs) == 2 && !isBlankIdent(node.Lhs[1]) {
					hasFragErrCheck = true
				}
			}
		case *ast.SelectorExpr:
			if id, ok := node.X.(*ast.Ident); ok && id.Name == "frag" {
				if node.Sel.Name == "SQL" {
					usesFragSQL = true
				}
				if node.Sel.Name == "Args" {
					usesFragArgs = true
				}
			}
		}
		return true
	})

	var violations []string
	if !hasEmptyPredCheck {
		violations = append(violations, "guardedCatalogueDB.prepare must reject empty spec.predicates")
	}
	if !hasFragCall || !hasFragErrCheck {
		violations = append(violations, "guardedCatalogueDB.prepare must call predicate fragmentFn and check its error")
	}
	if !usesFragSQL || !usesFragArgs {
		violations = append(violations, "guardedCatalogueDB.prepare must wire frag.SQL and frag.Args")
	}
	return violations
}

func verifyBoundaryExecCallsPrepareFirst(info *auditedFuncInfo) bool {
	var preparePos, dbCallPos token.Pos
	var prepareErrChecked bool

	ast.Inspect(info.decl.Body, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.AssignStmt:
			if len(node.Rhs) == 1 && isCallToSelector(node.Rhs[0], "prepare") {
				if len(node.Lhs) >= 3 && !isBlankIdent(node.Lhs[len(node.Lhs)-1]) {
					preparePos = node.Pos()
					prepareErrChecked = true
				}
			}
		case *ast.CallExpr:
			if sel, ok := node.Fun.(*ast.SelectorExpr); ok && dbExecutionMethods[sel.Sel.Name] {
				if dbCallPos == token.NoPos {
					dbCallPos = node.Pos()
				}
			}
		}
		return true
	})
	return preparePos != token.NoPos && prepareErrChecked && dbCallPos != token.NoPos && preparePos < dbCallPos
}

func inspectFuncStructure(info *auditedFuncInfo) {
	validPredSlices := collectValidPredicateSliceVars(info.decl.Body)

	ast.Inspect(info.decl.Body, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.AssignStmt:
			if len(node.Rhs) == 1 && isLoadPredicateCall(node.Rhs[0]) {
				if len(node.Lhs) != 2 || isBlankIdent(node.Lhs[0]) || isBlankIdent(node.Lhs[1]) {
					info.auditViolations = append(info.auditViolations,
						fmt.Sprintf("%s must retain both predicate and error from loadPredicate", info.key))
				}
				info.callsLoadPred = true
			}
		case *ast.ExprStmt:
			if isLoadPredicateCall(node.X) {
				info.auditViolations = append(info.auditViolations,
					fmt.Sprintf("%s discards result/error from loadPredicate", info.key))
				info.callsLoadPred = true
			}
		case *ast.CompositeLit:
			if isIdentNamed(node.Type, "guardedQuerySpec") {
				if reason := validateGuardedQuerySpec(node, validPredSlices); reason != "" {
					info.auditViolations = append(info.auditViolations,
						fmt.Sprintf("%s constructs invalid guardedQuerySpec: %s", info.key, reason))
				}
			}
		case *ast.CallExpr:
			recordCallInfo(info, node)
		}
		return true
	})
}

func recordCallInfo(info *auditedFuncInfo, call *ast.CallExpr) {
	switch fn := call.Fun.(type) {
	case *ast.Ident:
		info.directCalls = append(info.directCalls, fn.Name)
		if fn.Name == "New" {
			info.callsSQLCNew = true
		}
	case *ast.IndexExpr:
		if id, ok := fn.X.(*ast.Ident); ok {
			info.directCalls = append(info.directCalls, id.Name)
		}
	case *ast.IndexListExpr:
		if id, ok := fn.X.(*ast.Ident); ok {
			info.directCalls = append(info.directCalls, id.Name)
		}
	case *ast.SelectorExpr:
		if fn.Sel.Name == "loadPredicate" {
			info.callsLoadPred = true
		}
		if dbExecutionMethods[fn.Sel.Name] {
			info.rawDBCalls = append(info.rawDBCalls, fn.Sel.Name)
		}
		if target := resolveSelectorTarget(info, fn); target != "" {
			info.directCalls = append(info.directCalls, target)
		}
	}
}

func resolveSelectorTarget(caller *auditedFuncInfo, sel *ast.SelectorExpr) string {
	switch recv := sel.X.(type) {
	case *ast.Ident:
		if caller.receiverIdent != "" && recv.Name == caller.receiverIdent && caller.receiverType != "" {
			return caller.receiverType + "." + sel.Sel.Name
		}
		if typ := caller.paramTypes[recv.Name]; typ != "" {
			return typ + "." + sel.Sel.Name
		}
	case *ast.SelectorExpr:
		if id, ok := recv.X.(*ast.Ident); ok && caller.receiverIdent != "" && id.Name == caller.receiverIdent &&
			caller.receiverType == "CatalogueRepository" && recv.Sel.Name == "guard" {
			return "guardedCatalogueDB." + sel.Sel.Name
		}
	}
	return ""
}

func collectParamBaseTypes(fd *ast.FuncDecl) map[string]string {
	out := make(map[string]string)
	if fd.Type.Params == nil {
		return out
	}
	for _, field := range fd.Type.Params.List {
		base := baseTypeName(field.Type)
		if base == "" {
			continue
		}
		for _, name := range field.Names {
			out[name.Name] = base
		}
	}
	return out
}

func baseTypeName(expr ast.Expr) string {
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

func collectValidPredicateSliceVars(body *ast.BlockStmt) map[string]bool {
	valid := make(map[string]bool)
	invalidated := make(map[string]bool)

	ast.Inspect(body, func(n ast.Node) bool {
		assign, ok := n.(*ast.AssignStmt)
		if !ok || len(assign.Lhs) != 1 || len(assign.Rhs) != 1 {
			return true
		}
		lhsIdent, ok := assign.Lhs[0].(*ast.Ident)
		if !ok {
			return true
		}
		lit, ok := assign.Rhs[0].(*ast.CompositeLit)
		if !ok || !isPredicateClauseSliceType(lit.Type) {
			return true
		}
		if validatePredicateSliceLiteral(lit) == "" {
			valid[lhsIdent.Name] = true
		} else {
			invalidated[lhsIdent.Name] = true
		}
		return true
	})

	for name := range invalidated {
		delete(valid, name)
	}
	return valid
}

func validateGuardedQuerySpec(lit *ast.CompositeLit, validPredSlices map[string]bool) string {
	var predExpr ast.Expr
	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			return "unkeyed guardedQuerySpec literal"
		}
		if keyIdent, ok := kv.Key.(*ast.Ident); ok && keyIdent.Name == "predicates" {
			predExpr = kv.Value
			break
		}
	}
	if predExpr == nil {
		return "guardedQuerySpec missing predicates"
	}

	switch val := predExpr.(type) {
	case *ast.CompositeLit:
		return validatePredicateSliceLiteral(val)
	case *ast.Ident:
		if !validPredSlices[val.Name] {
			return fmt.Sprintf("predicates variable %q is not initialized with valid predicate clauses", val.Name)
		}
		return ""
	default:
		return "guardedQuerySpec predicates must be a slice literal or validated variable"
	}
}

func validatePredicateSliceLiteral(lit *ast.CompositeLit) string {
	if len(lit.Elts) == 0 {
		return "empty predicates slice"
	}
	for _, elt := range lit.Elts {
		if !isValidPredicateClauseExpr(elt) {
			return "invalid predicateClause element"
		}
	}
	return ""
}

func isValidPredicateClauseExpr(expr ast.Expr) bool {
	lit, ok := expr.(*ast.CompositeLit)
	if !ok {
		return false
	}
	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		keyIdent, ok := kv.Key.(*ast.Ident)
		if !ok || keyIdent.Name != "fragmentFn" {
			continue
		}
		if sel, ok := kv.Value.(*ast.SelectorExpr); ok && predicateFragmentMethods[sel.Sel.Name] {
			return true
		}
	}
	return false
}

func verifyExportedMethodUsesBoundary(method *auditedFuncInfo, funcs map[string]*auditedFuncInfo) []string {
	var violations []string
	if !method.hasCtxAndScope {
		violations = append(violations, fmt.Sprintf("%s lacks (ctx context.Context, scope storage.CatalogueReadScope) parameters", method.name))
	}

	reachable := collectReachableFuncs(method.key, funcs)
	usesLoadPred := false
	usesGuardedQuery := false

	keys := slices.Collect(maps.Keys(reachable))
	slices.Sort(keys)
	for _, k := range keys {
		f := funcs[k]
		if f == nil {
			continue
		}
		violations = append(violations, f.auditViolations...)
		if f.callsLoadPred {
			usesLoadPred = true
		}
		if guardedBoundaryQueries[k] {
			usesGuardedQuery = true
		}
		violations = append(violations, checkRawAccess(f)...)
	}

	if !usesLoadPred {
		violations = append(violations, fmt.Sprintf("%s does not load LabelPredicate via loadPredicate", method.name))
	}
	if !usesGuardedQuery {
		violations = append(violations, fmt.Sprintf("%s does not execute queries through guardedCatalogueDB", method.name))
	}
	return violations
}

func checkRawAccess(info *auditedFuncInfo) []string {
	if info.key == "LoadLabelPredicate" || guardedBoundaryQueries[info.key] {
		return nil
	}
	var violations []string
	for _, method := range info.rawDBCalls {
		violations = append(violations,
			fmt.Sprintf("%s executes raw DB method %s outside guardedCatalogueDB", info.key, method))
	}
	if info.callsSQLCNew {
		violations = append(violations, fmt.Sprintf("%s invokes sqlc New() directly outside LoadLabelPredicate", info.key))
	}
	return violations
}

func collectReachableFuncs(startKey string, funcs map[string]*auditedFuncInfo) map[string]bool {
	visited := make(map[string]bool)
	queue := []string{startKey}
	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]
		if visited[curr] {
			continue
		}
		visited[curr] = true
		if curr == "LoadLabelPredicate" || guardedBoundaryQueries[curr] {
			continue
		}
		info := funcs[curr]
		if info == nil {
			continue
		}
		for _, next := range info.directCalls {
			if !visited[next] {
				queue = append(queue, next)
			}
		}
	}
	return visited
}

func funcKey(receiverType, name string) string {
	if receiverType == "" {
		return name
	}
	return receiverType + "." + name
}

func receiverTypeAndIdent(fd *ast.FuncDecl) (string, string) {
	if fd.Recv == nil || len(fd.Recv.List) == 0 {
		return "", ""
	}
	field := fd.Recv.List[0]
	var identName string
	if len(field.Names) > 0 {
		identName = field.Names[0].Name
	}
	expr := field.Type
	for {
		switch t := expr.(type) {
		case *ast.StarExpr:
			expr = t.X
		case *ast.IndexExpr:
			expr = t.X
		case *ast.IndexListExpr:
			expr = t.X
		case *ast.Ident:
			return t.Name, identName
		default:
			return "", identName
		}
	}
}

func hasASTCtxAndScopeParams(fd *ast.FuncDecl) bool {
	if fd.Type.Params == nil {
		return false
	}
	var paramTypes []ast.Expr
	for _, field := range fd.Type.Params.List {
		n := max(len(field.Names), 1)
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
	id, ok := sel.X.(*ast.Ident)
	return ok && id.Name == pkg && sel.Sel.Name == name
}

func isLoadPredicateCall(expr ast.Expr) bool {
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	return ok && sel.Sel.Name == "loadPredicate"
}

func isCallToSelector(expr ast.Expr, name string) bool {
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return false
	}
	switch fn := call.Fun.(type) {
	case *ast.SelectorExpr:
		return fn.Sel.Name == name
	case *ast.Ident:
		return fn.Name == name
	default:
		return false
	}
}

func hasLenCallOn(expr ast.Expr, root, field string) bool {
	found := false
	ast.Inspect(expr, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || !isIdentNamed(call.Fun, "len") || len(call.Args) != 1 {
			return true
		}
		sel, ok := call.Args[0].(*ast.SelectorExpr)
		if ok && isIdentNamed(sel.X, root) && sel.Sel.Name == field {
			found = true
			return false
		}
		return true
	})
	return found
}

func isPredicateClauseSliceType(expr ast.Expr) bool {
	arr, ok := expr.(*ast.ArrayType)
	return ok && arr.Len == nil && isIdentNamed(arr.Elt, "predicateClause")
}

func isIdentNamed(expr ast.Expr, name string) bool {
	id, ok := expr.(*ast.Ident)
	return ok && id.Name == name
}

func isBlankIdent(expr ast.Expr) bool {
	return isIdentNamed(expr, "_")
}
