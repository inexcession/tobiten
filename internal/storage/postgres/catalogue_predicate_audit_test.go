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

type builderReturnSig struct {
	sqlRetIdx  int
	argsRetIdx int
	hasErrRet  bool
}

type passthroughSig struct {
	sqlRetToParam  map[int]int
	argsRetToParam map[int]int
}

type auditedFuncInfo struct {
	key             string
	name            string
	receiverType    string
	exported        bool
	hasCtxAndScope  bool
	decl            *ast.FuncDecl
	directCalls     []string
	fragmentCalls   []string
	callsLoadPred   bool
	callsDBQuery    bool
	callsSQLCNew    bool
	auditViolations []string
	builderSig      *builderReturnSig
	passthroughSig  *passthroughSig
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
	return scanSeriesRow(r.db.QueryRow(ctx, selectSeriesColumns+" WHERE s.id = $1", id))
}
`
		assertRejected(t, mutated, "GetSeriesUnsafe", "GetSeriesUnsafe without scope or predicate")
	})

	t.Run("rejects new read method with scope that skips LabelPredicate", func(t *testing.T) {
		mutated := string(baseSrc) + `
func (r *CatalogueRepository) ListSeriesBypass(ctx context.Context, scope storage.CatalogueReadScope) ([]domain.Series, error) {
	_, _ = r.db.Query(ctx, selectSeriesColumns)
	return nil, nil
}
`
		assertRejected(t, mutated, "ListSeriesBypass", "ListSeriesBypass skipping LabelPredicate")
	})

	t.Run("rejects new read method calling loadPredicate without fragment", func(t *testing.T) {
		mutated := string(baseSrc) + `
func (r *CatalogueRepository) CountSeriesNoFragment(ctx context.Context, scope storage.CatalogueReadScope) (int64, error) {
	if _, err := r.loadPredicate(ctx, scope); err != nil {
		return 0, err
	}
	return scanCountRow(r.db.QueryRow(ctx, "SELECT COUNT(*) FROM series s"))
}
`
		assertRejected(t, mutated, "CountSeriesNoFragment", "CountSeriesNoFragment without fragment call")
	})

	t.Run("rejects new read method discarding fragment error", func(t *testing.T) {
		mutated := string(baseSrc) + `
func (r *CatalogueRepository) CountSeriesDiscardedErr(ctx context.Context, scope storage.CatalogueReadScope) (int64, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return 0, err
	}
	frag, _ := pred.SeriesFragment("s", 1)
	return scanCountRow(r.db.QueryRow(ctx, "SELECT COUNT(*) FROM series s WHERE "+frag.SQL, frag.Args...))
}
`
		assertRejected(t, mutated, "CountSeriesDiscardedErr", "CountSeriesDiscardedErr ignoring fragment error")
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
	return scanCountRow(r.db.QueryRow(ctx, "SELECT COUNT(*) FROM series s"))
}
`
		assertRejected(t, mutated, "CountSeriesUnusedFragment", "CountSeriesUnusedFragment not wiring frag.SQL/frag.Args")
	})

	t.Run("rejects unguarded inline query in an otherwise guarded method", func(t *testing.T) {
		mutated := string(baseSrc) + `
func (r *CatalogueRepository) GetSeriesWithExtraUnguardedQuery(ctx context.Context, scope storage.CatalogueReadScope, id string) (*domain.Series, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return nil, err
	}
	frag, err := pred.SeriesFragment("s", 2)
	if err != nil {
		return nil, err
	}
	var extra string
	_ = r.db.QueryRow(ctx, "SELECT title FROM series WHERE id = $1", id).Scan(&extra)
	query := selectSeriesColumns + " WHERE s.id = $1 AND (" + frag.SQL + ")"
	args := append([]any{id}, frag.Args...)
	return scanSeriesRow(r.db.QueryRow(ctx, query, args...))
}
`
		assertRejected(t, mutated, "GetSeriesWithExtraUnguardedQuery executes unguarded QueryRow", "unguarded second QueryRow in an otherwise guarded method")
	})

	t.Run("rejects unguarded helper query called from an otherwise guarded method", func(t *testing.T) {
		mutated := string(baseSrc) + `
func (r *CatalogueRepository) GetVolumeWithUnguardedHelper(ctx context.Context, scope storage.CatalogueReadScope, id string) (*domain.Volume, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return nil, err
	}
	vol, err := r.getVolumeWithPredicate(ctx, pred, id)
	if err != nil {
		return nil, err
	}
	if err := r.unguardedLoadMembershipsHelper(ctx, vol.ID); err != nil {
		return nil, err
	}
	return vol, nil
}

func (r *CatalogueRepository) unguardedLoadMembershipsHelper(ctx context.Context, volumeID string) error {
	rows, err := r.db.Query(ctx, "SELECT series_id FROM series_memberships WHERE volume_id = $1", volumeID)
	if err != nil {
		return err
	}
	rows.Close()
	return nil
}
`
		assertRejected(t, mutated, "unguardedLoadMembershipsHelper executes unguarded Query", "unguarded helper query reachable from guarded method")
	})

	t.Run("rejects query wiring frag.SQL without frag.Args", func(t *testing.T) {
		mutated := string(baseSrc) + `
func (r *CatalogueRepository) CountSeriesMissingFragArgs(ctx context.Context, scope storage.CatalogueReadScope, id string) (int64, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return 0, err
	}
	frag, err := pred.SeriesFragment("s", 2)
	if err != nil {
		return 0, err
	}
	query := "SELECT COUNT(*) FROM series s WHERE s.id = $1 AND (" + frag.SQL + ")"
	_ = frag.Args
	return scanCountRow(r.db.QueryRow(ctx, query, id))
}
`
		assertRejected(t, mutated, "CountSeriesMissingFragArgs executes unguarded QueryRow", "query referencing frag.SQL without passing frag.Args")
	})

	t.Run("rejects volume query using joinClause without whereClause", func(t *testing.T) {
		mutated := string(baseSrc) + `
func (r *CatalogueRepository) ListVolumesDropsWhereClause(ctx context.Context, scope storage.CatalogueReadScope, filter storage.VolumeReadFilter) ([]domain.Volume, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return nil, err
	}
	joinClause, whereClause, args, err := buildVolumeFilterQuery(pred, filter)
	if err != nil {
		return nil, err
	}
	_, _ = whereClause, args
	rows, err := r.db.Query(ctx, selectVolumeColumns+joinClause)
	if err != nil {
		return nil, err
	}
	rows.Close()
	return nil, nil
}
`
		assertRejected(t, mutated, "ListVolumesDropsWhereClause executes unguarded Query", "query dropping whereClause from filter builder")
	})

	t.Run("rejects direct sqlc Queries bypass inside otherwise guarded method", func(t *testing.T) {
		mutated := string(baseSrc) + `
func (r *CatalogueRepository) GetSeriesWithSQLCBypass(ctx context.Context, scope storage.CatalogueReadScope, id string) (*domain.Series, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return nil, err
	}
	_, _ = New(r.db).GetUserByID(ctx, "u-1")
	frag, err := pred.SeriesFragment("s", 2)
	if err != nil {
		return nil, err
	}
	query := selectSeriesColumns + " WHERE s.id = $1 AND (" + frag.SQL + ")"
	args := append([]any{id}, frag.Args...)
	return scanSeriesRow(r.db.QueryRow(ctx, query, args...))
}
`
		assertRejected(t, mutated, "GetSeriesWithSQLCBypass invokes sqlc New()", "direct sqlc New(r.db) query inside guarded method")
	})

	t.Run("rejects filter builder with unguarded success before guarded success", func(t *testing.T) {
		mutated := string(baseSrc) + `
func buildSeriesFilterMixedBefore(pred *LabelPredicate, filter storage.SeriesReadFilter) (string, []any, error) {
	if filter.LibraryID != "" {
		return " WHERE s.library_id = 'bypass'", nil, nil
	}
	frag, err := pred.SeriesFragment("s", 1)
	if err != nil {
		return "", nil, err
	}
	return " WHERE (" + frag.SQL + ")", frag.Args, nil
}

func (r *CatalogueRepository) ListSeriesMixedBefore(ctx context.Context, scope storage.CatalogueReadScope, filter storage.SeriesReadFilter) ([]domain.Series, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return nil, err
	}
	whereClause, args, err := buildSeriesFilterMixedBefore(pred, filter)
	if err != nil {
		return nil, err
	}
	rows, err := r.db.Query(ctx, selectSeriesColumns+whereClause, args...)
	if err != nil {
		return nil, err
	}
	rows.Close()
	return nil, nil
}
`
		assertRejected(t, mutated, "buildSeriesFilterMixedBefore has successful return path without matching SQLFragment SQL and Args", "mixed builder with unguarded early return")
		assertRejected(t, mutated, "ListSeriesMixedBefore executes unguarded Query without matching predicate SQLFragment SQL and Args", "caller of mixed builder with unguarded early return")
	})

	t.Run("rejects filter builder with guarded success before unguarded success", func(t *testing.T) {
		mutated := string(baseSrc) + `
func buildSeriesFilterMixedAfter(pred *LabelPredicate, filter storage.SeriesReadFilter) (string, []any, error) {
	if filter.LibraryID != "" {
		frag, err := pred.SeriesFragment("s", 2)
		if err != nil {
			return "", nil, err
		}
		return " WHERE s.library_id = $1 AND (" + frag.SQL + ")", append([]any{filter.LibraryID}, frag.Args...), nil
	}
	return "", nil, nil
}

func (r *CatalogueRepository) ListSeriesMixedAfter(ctx context.Context, scope storage.CatalogueReadScope, filter storage.SeriesReadFilter) ([]domain.Series, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return nil, err
	}
	whereClause, args, err := buildSeriesFilterMixedAfter(pred, filter)
	if err != nil {
		return nil, err
	}
	rows, err := r.db.Query(ctx, selectSeriesColumns+whereClause, args...)
	if err != nil {
		return nil, err
	}
	rows.Close()
	return nil, nil
}
`
		assertRejected(t, mutated, "buildSeriesFilterMixedAfter has successful return path without matching SQLFragment SQL and Args", "mixed builder with unguarded fallback return")
		assertRejected(t, mutated, "ListSeriesMixedAfter executes unguarded Query without matching predicate SQLFragment SQL and Args", "caller of mixed builder with unguarded fallback return")
	})

	t.Run("rejects filter builder with unguarded errors.Join error return", func(t *testing.T) {
		mutated := string(baseSrc) + `
func buildSeriesFilterErrorsJoin(pred *LabelPredicate, filter storage.SeriesReadFilter) (string, []any, error) {
	if filter.LibraryID != "" {
		return " WHERE s.library_id = 'bypass'", nil, errors.Join(nil, nil)
	}
	frag, err := pred.SeriesFragment("s", 1)
	if err != nil {
		return "", nil, err
	}
	return " WHERE (" + frag.SQL + ")", frag.Args, nil
}

func (r *CatalogueRepository) ListSeriesErrorsJoin(ctx context.Context, scope storage.CatalogueReadScope, filter storage.SeriesReadFilter) ([]domain.Series, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return nil, err
	}
	whereClause, args, err := buildSeriesFilterErrorsJoin(pred, filter)
	if err != nil {
		return nil, err
	}
	rows, err := r.db.Query(ctx, selectSeriesColumns+whereClause, args...)
	if err != nil {
		return nil, err
	}
	rows.Close()
	return nil, nil
}
`
		assertRejected(t, mutated, "buildSeriesFilterErrorsJoin has successful return path without matching SQLFragment SQL and Args", "builder returning errors.Join without guard")
		assertRejected(t, mutated, "ListSeriesErrorsJoin executes unguarded Query without matching predicate SQLFragment SQL and Args", "caller of builder returning errors.Join")
	})

	t.Run("rejects filter builder with nil-valued error variable after else err!=nil branch", func(t *testing.T) {
		mutated := string(baseSrc) + `
func buildSeriesFilterElseErrLeak(pred *LabelPredicate, filter storage.SeriesReadFilter) (string, []any, error) {
	frag, err := pred.SeriesFragment("s", 1)
	if err == nil {
		_ = filter
	} else {
		return "", nil, err
	}
	var outErr error
	if filter.LibraryID != "" {
		return " WHERE s.library_id = 'bypass'", nil, outErr
	}
	_ = err
	return " WHERE (" + frag.SQL + ")", frag.Args, err
}

func (r *CatalogueRepository) ListSeriesElseErrLeak(ctx context.Context, scope storage.CatalogueReadScope, filter storage.SeriesReadFilter) ([]domain.Series, error) {
	pred, err := r.loadPredicate(ctx, scope)
	if err != nil {
		return nil, err
	}
	whereClause, args, err := buildSeriesFilterElseErrLeak(pred, filter)
	if err != nil {
		return nil, err
	}
	rows, err := r.db.Query(ctx, selectSeriesColumns+whereClause, args...)
	if err != nil {
		return nil, err
	}
	rows.Close()
	return nil, nil
}
`
		assertRejected(t, mutated, "buildSeriesFilterElseErrLeak has successful return path without matching SQLFragment SQL and Args", "builder returning nil-valued error variable after else branch")
		assertRejected(t, mutated, "ListSeriesElseErrLeak executes unguarded Query without matching predicate SQLFragment SQL and Args", "caller of builder with nil-valued error return")
	})

	t.Run("rejects passthrough helper with nil-valued error return", func(t *testing.T) {
		mutated := strings.Replace(
			string(baseSrc),
			"func appendPagination(query string, args []any, limit, offset int) (string, []any, error) {",
			`func appendPagination(query string, args []any, limit, offset int) (string, []any, error) {
	var sneakyErr error
	if limit == 999 {
		return "SELECT * FROM series", nil, sneakyErr
	}`,
			1,
		)
		assertRejected(t, mutated, "ListSeries executes unguarded Query without matching predicate SQLFragment SQL and Args", "caller of passthrough helper with nil-valued error bypass")
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
			recv := receiverTypeName(fd)
			key := funcKey(recv, fd.Name.Name)
			funcs[key] = &auditedFuncInfo{
				key:            key,
				name:           fd.Name.Name,
				receiverType:   recv,
				exported:       ast.IsExported(fd.Name.Name),
				hasCtxAndScope: hasASTCtxAndScopeParams(fd),
				decl:           fd,
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

	keys := make([]string, 0, len(funcs))
	for k := range funcs {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	// Pass 1: derive passthrough transforms before auditing builders and DB call sites.
	for _, k := range keys {
		if funcs[k].receiverType == "" {
			analyzeHelperSignatures(funcs[k])
		}
	}

	// Pass 2: analyze package-level *LabelPredicate filter builders first so their return taint signatures are known.
	for _, k := range keys {
		info := funcs[k]
		if info.receiverType == "" && acceptsLabelPredicateParam(info.decl) {
			auditFuncBody(info, funcs)
		}
	}

	// Pass 3: run statement-level taint and per-query guard audit on all remaining functions.
	for _, k := range keys {
		info := funcs[k]
		if info.receiverType != "" || !acceptsLabelPredicateParam(info.decl) {
			auditFuncBody(info, funcs)
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
	paramTypes := flattenParamTypes(fd.Type.Params)
	if len(paramTypes) < 2 {
		return false
	}
	return isSelectorType(paramTypes[0], "context", "Context") &&
		isSelectorType(paramTypes[1], "storage", "CatalogueReadScope")
}

func flattenParamTypes(params *ast.FieldList) []ast.Expr {
	if params == nil {
		return nil
	}
	var out []ast.Expr
	for _, field := range params.List {
		n := max(len(field.Names), 1)
		for range n {
			out = append(out, field.Type)
		}
	}
	return out
}

func flattenParamNames(params *ast.FieldList) []string {
	if params == nil {
		return nil
	}
	var out []string
	for _, field := range params.List {
		if len(field.Names) == 0 {
			out = append(out, "")
			continue
		}
		for _, ident := range field.Names {
			out = append(out, ident.Name)
		}
	}
	return out
}

func acceptsLabelPredicateParam(fd *ast.FuncDecl) bool {
	for _, typ := range flattenParamTypes(fd.Type.Params) {
		if star, ok := typ.(*ast.StarExpr); ok {
			if ident, ok := star.X.(*ast.Ident); ok && ident.Name == "LabelPredicate" {
				return true
			}
		}
	}
	return false
}

func isSelectorType(expr ast.Expr, pkg, name string) bool {
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	ident, ok := sel.X.(*ast.Ident)
	return ok && ident.Name == pkg && sel.Sel.Name == name
}

type taintSet map[int]bool

func cloneTaintSet(in taintSet) taintSet {
	if len(in) == 0 {
		return nil
	}
	return maps.Clone(in)
}

func unionTaintSets(sets ...taintSet) taintSet {
	var out taintSet
	for _, s := range sets {
		for id := range s {
			if out == nil {
				out = make(taintSet)
			}
			out[id] = true
		}
	}
	return out
}

func intersectTaintSets(a, b taintSet) taintSet {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}
	var out taintSet
	for id := range a {
		if b[id] {
			if out == nil {
				out = make(taintSet)
			}
			out[id] = true
		}
	}
	return out
}

type funcTaintState struct {
	info                      *auditedFuncInfo
	funcs                     map[string]*auditedFuncInfo
	sqlVars                   map[string]taintSet
	argsVars                  map[string]taintSet
	fragVarsSQL               map[string]int
	fragVarsArgs              map[string]int
	nextSourceID              int
	consumedByQuery           map[int]bool
	isBuilder                 bool
	isPassthrough             bool
	nonNilErrs                map[string]bool
	builderFailed             bool
	builderSuccessReturns     int
	passthroughFailed         bool
	passthroughSuccessReturns int
	passthroughSigCandidate   *builderReturnSig
}

func analyzeHelperSignatures(info *auditedFuncInfo) {
	if info.decl.Type.Results == nil || len(info.decl.Type.Results.List) < 2 {
		return
	}
	if acceptsLabelPredicateParam(info.decl) {
		return
	}

	paramNames := flattenParamNames(info.decl.Type.Params)
	if len(paramNames) < 2 || paramNames[0] == "" || paramNames[1] == "" {
		return
	}

	hasDBCall := false
	ast.Inspect(info.decl.Body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			target := callTargetName(call.Fun)
			if dbExecutionMethods[target] || target == "LoadLabelPredicate" || target == "New" {
				hasDBCall = true
			}
		}
		return true
	})
	if hasDBCall {
		return
	}

	st := &funcTaintState{
		info: info,
		sqlVars: map[string]taintSet{
			paramNames[0]: {1: true},
		},
		argsVars: map[string]taintSet{
			paramNames[1]: {1: true},
		},
		consumedByQuery: make(map[int]bool),
		isPassthrough:   true,
		nonNilErrs:      make(map[string]bool),
	}
	st.walkBlock(info.decl.Body)

	if st.passthroughFailed || st.passthroughSuccessReturns == 0 || st.passthroughSigCandidate == nil {
		return
	}
	info.passthroughSig = &passthroughSig{
		sqlRetToParam:  map[int]int{st.passthroughSigCandidate.sqlRetIdx: 0},
		argsRetToParam: map[int]int{st.passthroughSigCandidate.argsRetIdx: 1},
	}
}

func auditFuncBody(info *auditedFuncInfo, funcs map[string]*auditedFuncInfo) {
	st := &funcTaintState{
		info:            info,
		funcs:           funcs,
		sqlVars:         make(map[string]taintSet),
		argsVars:        make(map[string]taintSet),
		fragVarsSQL:     make(map[string]int),
		fragVarsArgs:    make(map[string]int),
		consumedByQuery: make(map[int]bool),
		isBuilder:       acceptsLabelPredicateParam(info.decl) && info.receiverType == "",
		nonNilErrs:      make(map[string]bool),
	}
	st.walkBlock(info.decl.Body)

	if st.isBuilder && (st.builderFailed || st.builderSuccessReturns == 0) {
		info.builderSig = nil
	}

	// Verify that every fragment generated directly in a non-builder function was wired into at least one query.
	if !st.isBuilder {
		for fragName, srcID := range st.fragVarsSQL {
			if !st.consumedByQuery[srcID] {
				info.auditViolations = append(info.auditViolations,
					fmt.Sprintf("%s creates SQLFragment %q without wiring both .SQL and .Args into a query", info.name, fragName))
			}
		}
	}
}

func (st *funcTaintState) allocSourceID() int {
	st.nextSourceID++
	return st.nextSourceID
}

func (st *funcTaintState) snapshotVars() (map[string]taintSet, map[string]taintSet) {
	sqlCopy := make(map[string]taintSet, len(st.sqlVars))
	for k, v := range st.sqlVars {
		sqlCopy[k] = cloneTaintSet(v)
	}
	argsCopy := make(map[string]taintSet, len(st.argsVars))
	for k, v := range st.argsVars {
		argsCopy[k] = cloneTaintSet(v)
	}
	return sqlCopy, argsCopy
}

func (st *funcTaintState) intersectVarMaps(sqlOther, argsOther map[string]taintSet) {
	for k, s := range st.sqlVars {
		st.sqlVars[k] = intersectTaintSets(s, sqlOther[k])
	}
	for k, s := range st.argsVars {
		st.argsVars[k] = intersectTaintSets(s, argsOther[k])
	}
}

func (st *funcTaintState) walkBlock(block *ast.BlockStmt) {
	if block == nil {
		return
	}
	for _, stmt := range block.List {
		st.walkStmt(stmt)
	}
}

func (st *funcTaintState) walkStmt(stmt ast.Stmt) {
	if stmt == nil {
		return
	}
	switch s := stmt.(type) {
	case *ast.BlockStmt:
		st.walkBlock(s)
	case *ast.DeclStmt:
		st.walkDeclStmt(s)
	case *ast.AssignStmt:
		st.walkAssignStmt(s)
	case *ast.ExprStmt:
		st.checkBarePredicateCallExpr(s.X)
		st.scanCallsInExpr(s.X)
	case *ast.ReturnStmt:
		st.walkReturnStmt(s)
	case *ast.IfStmt:
		st.walkIfStmt(s)
	case *ast.ForStmt:
		if s.Init != nil {
			st.walkStmt(s.Init)
		}
		st.scanCallsInExpr(s.Cond)
		if s.Post != nil {
			st.walkStmt(s.Post)
		}
		st.walkBlock(s.Body)
	case *ast.RangeStmt:
		st.scanCallsInExpr(s.X)
		st.walkBlock(s.Body)
	case *ast.SwitchStmt:
		if s.Init != nil {
			st.walkStmt(s.Init)
		}
		st.scanCallsInExpr(s.Tag)
		st.walkBlock(s.Body)
	case *ast.CaseClause:
		for _, e := range s.List {
			st.scanCallsInExpr(e)
		}
		for _, sub := range s.Body {
			st.walkStmt(sub)
		}
	case *ast.DeferStmt:
		st.scanCallsInExpr(s.Call)
	case *ast.GoStmt:
		st.scanCallsInExpr(s.Call)
	}
}

func cloneErrMap(in map[string]bool) map[string]bool {
	out := make(map[string]bool, len(in))
	maps.Copy(out, in)
	return out
}

func (st *funcTaintState) walkIfStmt(s *ast.IfStmt) {
	if s.Init != nil {
		st.walkStmt(s.Init)
	}
	st.scanCallsInExpr(s.Cond)

	preSQL, preArgs := st.snapshotVars()
	preErrs := cloneErrMap(st.nonNilErrs)

	thenErrs := cloneErrMap(preErrs)
	if errVar, ok := condNonNilErrIdent(s.Cond, token.NEQ); ok {
		thenErrs[errVar] = true
	}
	st.nonNilErrs = thenErrs
	thenTerminates := blockAlwaysReturns(s.Body)
	st.walkBlock(s.Body)
	thenSQL, thenArgs := st.snapshotVars()

	if s.Else == nil {
		st.nonNilErrs = preErrs
		if thenTerminates {
			// Early return on `if`: subsequent execution stays on pre-branch state.
			st.sqlVars, st.argsVars = preSQL, preArgs
		} else {
			st.sqlVars, st.argsVars = preSQL, preArgs
			st.intersectVarMaps(thenSQL, thenArgs)
		}
		return
	}

	st.sqlVars, st.argsVars = preSQL, preArgs
	elseErrs := cloneErrMap(preErrs)
	if errVar, ok := condNonNilErrIdent(s.Cond, token.EQL); ok {
		elseErrs[errVar] = true
	}
	st.nonNilErrs = elseErrs
	elseTerminates := stmtAlwaysReturns(s.Else)
	st.walkStmt(s.Else)
	st.nonNilErrs = preErrs
	if thenTerminates && !elseTerminates {
		return
	}
	if !thenTerminates && elseTerminates {
		st.sqlVars, st.argsVars = thenSQL, thenArgs
		return
	}
	st.intersectVarMaps(thenSQL, thenArgs)
}

func condNonNilErrIdent(cond ast.Expr, op token.Token) (string, bool) {
	bin, ok := cond.(*ast.BinaryExpr)
	if !ok || bin.Op != op {
		return "", false
	}
	if id, ok := bin.X.(*ast.Ident); ok && isNilIdent(bin.Y) {
		return id.Name, true
	}
	if id, ok := bin.Y.(*ast.Ident); ok && isNilIdent(bin.X) {
		return id.Name, true
	}
	return "", false
}

func isNilIdent(expr ast.Expr) bool {
	id, ok := expr.(*ast.Ident)
	return ok && id.Name == "nil"
}

func blockAlwaysReturns(b *ast.BlockStmt) bool {
	if b == nil || len(b.List) == 0 {
		return false
	}
	return stmtAlwaysReturns(b.List[len(b.List)-1])
}

func stmtAlwaysReturns(stmt ast.Stmt) bool {
	switch s := stmt.(type) {
	case *ast.ReturnStmt:
		return true
	case *ast.BlockStmt:
		return blockAlwaysReturns(s)
	case *ast.IfStmt:
		return s.Else != nil && blockAlwaysReturns(s.Body) && stmtAlwaysReturns(s.Else)
	}
	return false
}

func (st *funcTaintState) walkDeclStmt(s *ast.DeclStmt) {
	gen, ok := s.Decl.(*ast.GenDecl)
	if !ok {
		return
	}
	for _, spec := range gen.Specs {
		vs, ok := spec.(*ast.ValueSpec)
		if !ok {
			continue
		}
		for i, name := range vs.Names {
			delete(st.nonNilErrs, name.Name)
			if i < len(vs.Values) {
				st.scanCallsInExpr(vs.Values[i])
				st.sqlVars[name.Name] = st.evalExprSQL(vs.Values[i])
				st.argsVars[name.Name] = st.evalExprArgs(vs.Values[i])
			} else {
				delete(st.sqlVars, name.Name)
				delete(st.argsVars, name.Name)
			}
		}
	}
}

func (st *funcTaintState) walkAssignStmt(assign *ast.AssignStmt) {
	for _, rhs := range assign.Rhs {
		st.scanCallsInExpr(rhs)
	}
	for _, lhs := range assign.Lhs {
		if ident, ok := lhs.(*ast.Ident); ok {
			delete(st.nonNilErrs, ident.Name)
		}
	}

	if len(assign.Rhs) == 1 {
		if call, ok := assign.Rhs[0].(*ast.CallExpr); ok {
			if st.handleSpecialAssignCall(assign.Lhs, call) {
				return
			}
		}
	}

	if len(assign.Rhs) == len(assign.Lhs) {
		newSQL := make([]taintSet, len(assign.Lhs))
		newArgs := make([]taintSet, len(assign.Lhs))
		for i, rhs := range assign.Rhs {
			newSQL[i] = st.evalExprSQL(rhs)
			newArgs[i] = st.evalExprArgs(rhs)
		}
		for i, lhs := range assign.Lhs {
			ident, ok := lhs.(*ast.Ident)
			if !ok || ident.Name == "_" {
				continue
			}
			if assign.Tok == token.ADD_ASSIGN {
				st.sqlVars[ident.Name] = unionTaintSets(st.sqlVars[ident.Name], newSQL[i])
				st.argsVars[ident.Name] = unionTaintSets(st.argsVars[ident.Name], newArgs[i])
			} else {
				st.sqlVars[ident.Name] = newSQL[i]
				st.argsVars[ident.Name] = newArgs[i]
			}
		}
		return
	}

	// Unmatched multi-return call: clear all assigned variables unless matched above.
	for _, lhs := range assign.Lhs {
		if ident, ok := lhs.(*ast.Ident); ok {
			delete(st.sqlVars, ident.Name)
			delete(st.argsVars, ident.Name)
		}
	}
}

func (st *funcTaintState) handleSpecialAssignCall(lhs []ast.Expr, call *ast.CallExpr) bool {
	target := callTargetName(call.Fun)
	if isPredicateOrFragTarget(target) {
		if len(lhs) < 2 {
			st.info.auditViolations = append(st.info.auditViolations,
				fmt.Sprintf("%s does not capture error from %s", st.info.name, target))
			return true
		}
		if errIdent, ok := lhs[len(lhs)-1].(*ast.Ident); ok && errIdent.Name == "_" {
			st.info.auditViolations = append(st.info.auditViolations,
				fmt.Sprintf("%s discards error from %s using _", st.info.name, target))
		}
		if predicateFragmentMethods[target] || target == "fragmentFn" {
			fragIdent, ok := lhs[0].(*ast.Ident)
			if !ok || fragIdent.Name == "_" {
				st.info.auditViolations = append(st.info.auditViolations,
					fmt.Sprintf("%s discards SQLFragment from %s using _", st.info.name, target))
				return true
			}
			srcID := st.allocSourceID()
			st.fragVarsSQL[fragIdent.Name] = srcID
			st.fragVarsArgs[fragIdent.Name] = srcID
		}
		return true
	}

	callee := st.funcs[funcKey("", target)]
	if callee == nil {
		callee = st.funcs[funcKey("CatalogueRepository", target)]
	}
	if callee == nil {
		return false
	}

	if callee.builderSig != nil {
		if len(lhs) >= 2 {
			if errIdent, ok := lhs[len(lhs)-1].(*ast.Ident); ok && errIdent.Name == "_" {
				st.info.auditViolations = append(st.info.auditViolations,
					fmt.Sprintf("%s discards error from %s using _", st.info.name, target))
			}
		}
		srcID := st.allocSourceID()
		for i, expr := range lhs {
			ident, ok := expr.(*ast.Ident)
			if !ok || ident.Name == "_" {
				continue
			}
			delete(st.sqlVars, ident.Name)
			delete(st.argsVars, ident.Name)
			if i == callee.builderSig.sqlRetIdx {
				st.sqlVars[ident.Name] = taintSet{srcID: true}
			}
			if i == callee.builderSig.argsRetIdx {
				st.argsVars[ident.Name] = taintSet{srcID: true}
			}
		}
		return true
	}

	if callee.passthroughSig != nil {
		sqlOutputs := make(map[int]taintSet)
		argsOutputs := make(map[int]taintSet)
		for retIdx, pIdx := range callee.passthroughSig.sqlRetToParam {
			if pIdx < len(call.Args) {
				sqlOutputs[retIdx] = st.evalExprSQL(call.Args[pIdx])
			}
		}
		for retIdx, pIdx := range callee.passthroughSig.argsRetToParam {
			if pIdx < len(call.Args) {
				argsOutputs[retIdx] = st.evalExprArgs(call.Args[pIdx])
			}
		}
		for i, expr := range lhs {
			ident, ok := expr.(*ast.Ident)
			if !ok || ident.Name == "_" {
				continue
			}
			st.sqlVars[ident.Name] = sqlOutputs[i]
			st.argsVars[ident.Name] = argsOutputs[i]
		}
		return true
	}

	return false
}

func (st *funcTaintState) walkReturnStmt(ret *ast.ReturnStmt) {
	for _, expr := range ret.Results {
		st.scanCallsInExpr(expr)
	}
	if st.isPassthrough {
		st.checkPassthroughReturn(ret.Results)
		return
	}
	if !st.isBuilder {
		return
	}
	if len(ret.Results) < 2 {
		st.failBuilder("has return without expected (sql, args, err) tuple")
		return
	}
	if isExplicitErrorExpr(ret.Results[len(ret.Results)-1], st.nonNilErrs) {
		return
	}

	st.builderSuccessReturns++
	sqlIdx, argsIdx, shared := st.inspectTupleTaint(ret.Results)
	if sqlIdx < 0 || argsIdx < 0 || len(shared) == 0 {
		st.failBuilder("has successful return path without matching SQLFragment SQL and Args")
		return
	}
	if st.builderFailed {
		return
	}
	if st.info.builderSig == nil {
		st.info.builderSig = &builderReturnSig{
			sqlRetIdx:  sqlIdx,
			argsRetIdx: argsIdx,
			hasErrRet:  true,
		}
		return
	}
	if st.info.builderSig.sqlRetIdx != sqlIdx || st.info.builderSig.argsRetIdx != argsIdx {
		st.failBuilder("returns predicate SQL/Args at inconsistent tuple positions across branches")
	}
}

func (st *funcTaintState) checkPassthroughReturn(results []ast.Expr) {
	if len(results) < 2 {
		st.passthroughFailed = true
		return
	}
	if isExplicitErrorExpr(results[len(results)-1], st.nonNilErrs) {
		return
	}
	st.passthroughSuccessReturns++
	sqlIdx, argsIdx, shared := st.inspectTupleTaint(results)
	if sqlIdx < 0 || argsIdx < 0 || len(shared) == 0 {
		st.passthroughFailed = true
		return
	}
	if st.passthroughSigCandidate == nil {
		st.passthroughSigCandidate = &builderReturnSig{
			sqlRetIdx:  sqlIdx,
			argsRetIdx: argsIdx,
			hasErrRet:  true,
		}
		return
	}
	if st.passthroughSigCandidate.sqlRetIdx != sqlIdx || st.passthroughSigCandidate.argsRetIdx != argsIdx {
		st.passthroughFailed = true
	}
}

func (st *funcTaintState) failBuilder(reason string) {
	st.builderFailed = true
	st.info.builderSig = nil
	st.info.auditViolations = append(st.info.auditViolations, st.info.name+" "+reason)
}

func isExplicitErrorExpr(errExpr ast.Expr, nonNilErrs map[string]bool) bool {
	if errExpr == nil || isNilIdent(errExpr) {
		return false
	}
	if ident, ok := errExpr.(*ast.Ident); ok {
		return nonNilErrs[ident.Name]
	}
	call, ok := errExpr.(*ast.CallExpr)
	if !ok {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	if !ok {
		return false
	}
	return (pkg.Name == "fmt" && sel.Sel.Name == "Errorf") ||
		(pkg.Name == "errors" && sel.Sel.Name == "New")
}

func (st *funcTaintState) inspectTupleTaint(results []ast.Expr) (int, int, taintSet) {
	sqlIdx, argsIdx := -1, -1
	var sqlTaint, argsTaint taintSet
	for i := range len(results) - 1 {
		s := st.evalExprSQL(results[i])
		a := st.evalExprArgs(results[i])
		if len(s) > 0 {
			if sqlIdx >= 0 {
				return -1, -1, nil
			}
			sqlIdx = i
			sqlTaint = s
		}
		if len(a) > 0 {
			if argsIdx >= 0 {
				return -1, -1, nil
			}
			argsIdx = i
			argsTaint = a
		}
	}
	return sqlIdx, argsIdx, intersectTaintSets(sqlTaint, argsTaint)
}

func (st *funcTaintState) checkBarePredicateCallExpr(expr ast.Expr) {
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return
	}
	if name := callTargetName(call.Fun); isPredicateOrFragTarget(name) {
		st.info.auditViolations = append(st.info.auditViolations,
			fmt.Sprintf("%s ignores return values of %s", st.info.name, name))
	}
}

func (st *funcTaintState) scanCallsInExpr(expr ast.Expr) {
	if expr == nil {
		return
	}
	ast.Inspect(expr, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		st.recordCall(call)
		return true
	})
}

func (st *funcTaintState) recordCall(call *ast.CallExpr) {
	recordName := func(target string) {
		if target == "" {
			return
		}
		if target == "LoadLabelPredicate" {
			st.info.callsLoadPred = true
		}
		if predicateFragmentMethods[target] {
			st.info.fragmentCalls = append(st.info.fragmentCalls, target)
		}
		if target == "Query" || target == "QueryRow" {
			st.info.callsDBQuery = true
		}
		st.info.directCalls = append(st.info.directCalls, target)
	}

	target := callTargetName(call.Fun)
	recordName(target)
	for _, arg := range call.Args {
		if argTarget := exprTargetName(arg); argTarget != "" {
			recordName(argTarget)
		}
	}

	if target == "New" {
		if ident, ok := call.Fun.(*ast.Ident); ok && ident.Name == "New" {
			st.info.callsSQLCNew = true
			st.info.auditViolations = append(st.info.auditViolations,
				fmt.Sprintf("%s invokes sqlc New() directly instead of predicate-enforced queries", st.info.name))
		}
	}

	if dbExecutionMethods[target] {
		st.verifyDBCallSite(target, call)
	}
}

func (st *funcTaintState) verifyDBCallSite(target string, call *ast.CallExpr) {
	// Skip LoadLabelPredicate itself; it loads user_label_policies as the predicate source.
	if st.info.name == "LoadLabelPredicate" && st.info.receiverType == "" {
		return
	}
	if target != "Query" && target != "QueryRow" {
		st.info.auditViolations = append(st.info.auditViolations,
			fmt.Sprintf("%s executes non-read DB call %s", st.info.name, target))
		return
	}
	if len(call.Args) < 2 {
		st.info.auditViolations = append(st.info.auditViolations,
			fmt.Sprintf("%s executes malformed %s call", st.info.name, target))
		return
	}

	sqlSources := st.evalExprSQL(call.Args[1])
	var argSets []taintSet
	for _, argExpr := range call.Args[2:] {
		argSets = append(argSets, st.evalExprArgs(argExpr))
	}
	argsSources := unionTaintSets(argSets...)
	shared := intersectTaintSets(sqlSources, argsSources)
	if len(shared) == 0 {
		st.info.auditViolations = append(st.info.auditViolations,
			fmt.Sprintf("%s executes unguarded %s without matching predicate SQLFragment SQL and Args", st.info.name, target))
		return
	}
	for id := range shared {
		st.consumedByQuery[id] = true
	}
}

func (st *funcTaintState) evalExprSQL(expr ast.Expr) taintSet {
	if expr == nil {
		return nil
	}
	switch e := expr.(type) {
	case *ast.Ident:
		return st.sqlVars[e.Name]
	case *ast.ParenExpr:
		return st.evalExprSQL(e.X)
	case *ast.BinaryExpr:
		return unionTaintSets(st.evalExprSQL(e.X), st.evalExprSQL(e.Y))
	case *ast.SelectorExpr:
		if ident, ok := e.X.(*ast.Ident); ok && e.Sel.Name == "SQL" {
			if id, exists := st.fragVarsSQL[ident.Name]; exists {
				return taintSet{id: true}
			}
		}
		return nil
	case *ast.CallExpr:
		if isAllowedSQLCombineCall(e.Fun) {
			sets := make([]taintSet, 0, len(e.Args))
			for _, a := range e.Args {
				sets = append(sets, st.evalExprSQL(a))
			}
			return unionTaintSets(sets...)
		}
		return nil
	case *ast.CompositeLit:
		sets := make([]taintSet, 0, len(e.Elts))
		for _, elt := range e.Elts {
			sets = append(sets, st.evalExprSQL(elt))
		}
		return unionTaintSets(sets...)
	case *ast.SliceExpr:
		return st.evalExprSQL(e.X)
	}
	return nil
}

func (st *funcTaintState) evalExprArgs(expr ast.Expr) taintSet {
	if expr == nil {
		return nil
	}
	switch e := expr.(type) {
	case *ast.Ident:
		return st.argsVars[e.Name]
	case *ast.ParenExpr:
		return st.evalExprArgs(e.X)
	case *ast.SelectorExpr:
		if ident, ok := e.X.(*ast.Ident); ok && e.Sel.Name == "Args" {
			if id, exists := st.fragVarsArgs[ident.Name]; exists {
				return taintSet{id: true}
			}
		}
		return nil
	case *ast.CallExpr:
		if ident, ok := e.Fun.(*ast.Ident); ok && ident.Name == "append" {
			sets := make([]taintSet, 0, len(e.Args))
			for _, a := range e.Args {
				sets = append(sets, st.evalExprArgs(a))
			}
			return unionTaintSets(sets...)
		}
		return nil
	case *ast.CompositeLit:
		sets := make([]taintSet, 0, len(e.Elts))
		for _, elt := range e.Elts {
			sets = append(sets, st.evalExprArgs(elt))
		}
		return unionTaintSets(sets...)
	case *ast.SliceExpr:
		return st.evalExprArgs(e.X)
	}
	return nil
}

func isAllowedSQLCombineCall(fun ast.Expr) bool {
	if ident, ok := fun.(*ast.Ident); ok && ident.Name == "append" {
		return true
	}
	sel, ok := fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	if !ok {
		return false
	}
	return (pkg.Name == "fmt" && sel.Sel.Name == "Sprintf") ||
		(pkg.Name == "strings" && sel.Sel.Name == "Join")
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
		// Only record per-function violations if this is not LoadLabelPredicate itself.
		if info.key != "LoadLabelPredicate" {
			violations = append(violations, info.auditViolations...)
		}

		for _, callee := range info.directCalls {
			if callee == "LoadLabelPredicate" {
				// LoadLabelPredicate is verified separately by TestLabelPredicate_*; don't walk into sqlc New().
				continue
			}
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
