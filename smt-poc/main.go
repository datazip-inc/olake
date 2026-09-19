// flatten-poc v3: flatten() expands to leaf columns in SQL; depth is a structure edit.
//
// Per batch:
//  1. json_group_structure(doc)           -> this batch's shape + types
//  2. merge into a running structure       -> never shrinks, types only widen
//  3. parse flatten(path, depth) terms     -> per-path depth map
//  4. cut(structure, depths)               -> objects past their limit become "JSON" leaves
//  5. rewrite each flatten() term          -> one projection per leaf of the CUT structure
//     (__o."customer"."addr"."city" AS customer_addr_city)
//  6. WITH batch AS (SELECT ..., json_transform(doc,'<cut>') AS __o ...) <user query>
//
// Everything the user does not flatten() is still addressable by name
// (customer.email, status, ...) because the CTE also exposes __o.*.
//
// Build:  go build -tags duckdb_arrow -o flatten-poc .
// Run:    ./flatten-poc -cases
//
//	./flatten-poc -source orders.jsonl -col doc -query "select flatten(customer, 2), status from batch"
package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/arrow-go/v18/parquet/variant"
	"github.com/marcboeker/go-duckdb/v2"
)

// ---------------------------------------------------------------------------
// structure tree
// ---------------------------------------------------------------------------

type kind int

const (
	kLeaf kind = iota
	kObject
	kArray
)

type node struct {
	kind   kind
	leaf   string // DuckDB type; "NULL" = unknown (wildcard); "JSON" = raw text
	fields map[string]*node
	order  []string
	elem   *node
}

func parseStructure(v any) *node {
	switch t := v.(type) {
	case map[string]any:
		n := &node{kind: kObject, fields: map[string]*node{}}
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			n.fields[k] = parseStructure(t[k])
			n.order = append(n.order, k)
		}
		return n
	case []any:
		n := &node{kind: kArray}
		if len(t) > 0 {
			n.elem = parseStructure(t[0])
		} else {
			n.elem = &node{kind: kLeaf, leaf: "NULL"}
		}
		return n
	case string:
		return &node{kind: kLeaf, leaf: t}
	default:
		return &node{kind: kLeaf, leaf: "JSON"}
	}
}

// resolver reports whether every non-null value at path in the current batch has the
// given JSON type ("OBJECT"/"ARRAY"). Used to disambiguate DuckDB's "JSON" verdict,
// which it also gives for {} and [] (shape unknown, not a conflict).
type resolver func(path []string, want string) bool

// merge folds b into a: keys only get added, types only get wider.
func merge(a, b *node, path []string, same resolver) *node {
	switch {
	case a == nil:
		return b
	case b == nil:
		return a
	case a.kind == kLeaf && a.leaf == "NULL":
		return b
	case b.kind == kLeaf && b.leaf == "NULL":
		return a
	case a.kind != b.kind:
		if b.kind == kLeaf && b.leaf == "JSON" && same != nil {
			want := "OBJECT"
			if a.kind == kArray {
				want = "ARRAY"
			}
			if same(path, want) {
				return a // {} or [] in this batch: keep the known shape
			}
		}
		return &node{kind: kLeaf, leaf: "JSON"}
	}
	switch a.kind {
	case kLeaf:
		if a.leaf == b.leaf {
			return a
		}
		return &node{kind: kLeaf, leaf: widen(a.leaf, b.leaf)}
	case kArray:
		return &node{kind: kArray, elem: merge(a.elem, b.elem, path, same)}
	default:
		out := &node{kind: kObject, fields: map[string]*node{}}
		for _, k := range a.order {
			out.fields[k] = merge(a.fields[k], b.fields[k], append(append([]string{}, path...), k), same)
			out.order = append(out.order, k)
		}
		for _, k := range b.order {
			if _, seen := out.fields[k]; !seen {
				out.fields[k] = b.fields[k]
				out.order = append(out.order, k)
			}
		}
		return out
	}
}

// widen: minimal promotion ladder (OLake: isValidTransition / getCommonAncestorType).
func widen(a, b string) string {
	rank := map[string]int{"UBIGINT": 1, "BIGINT": 2, "HUGEINT": 3, "UHUGEINT": 3, "DOUBLE": 4}
	ra, oka := rank[a]
	rb, okb := rank[b]
	if oka && okb {
		if ra > rb {
			return a
		}
		return b
	}
	return "JSON" // bool vs number, number vs string, ...: keep raw, never coerce silently
}

// cut enforces depth: objects at/after the limit become "JSON" leaves.
// depths: dotted path -> depth override; "" = default depth.
func cut(n *node, prefix string, depth int, depths map[string]int) *node {
	maxDepth := depths[""]
	if lim, ok := depths[prefix]; ok && prefix != "" {
		maxDepth, depth = lim, 0 // override restarts counting at this subtree
	}
	switch n.kind {
	case kArray:
		return &node{kind: kArray, elem: cutInner(n.elem, prefix, depth, maxDepth, depths)}
	case kObject:
		if depth >= maxDepth || len(n.order) == 0 {
			return &node{kind: kLeaf, leaf: "JSON"}
		}
		out := &node{kind: kObject, fields: map[string]*node{}, order: n.order}
		for _, k := range n.order {
			out.fields[k] = cutInner(n.fields[k], join(prefix, k), depth+1, maxDepth, depths)
		}
		return out
	default:
		return n
	}
}

// cutInner keeps the inherited maxDepth unless the child path has its own override.
func cutInner(n *node, prefix string, depth, maxDepth int, depths map[string]int) *node {
	if lim, ok := depths[prefix]; ok {
		maxDepth, depth = lim, 0
	}
	switch n.kind {
	case kArray:
		return &node{kind: kArray, elem: cutInner(n.elem, prefix, depth, maxDepth, depths)}
	case kObject:
		// an ancestor of a deeper flatten(path) stays open so the override is reachable;
		// its other children still obey the inherited limit
		if (depth >= maxDepth && !overrideBelow(prefix, depths)) || len(n.order) == 0 {
			return &node{kind: kLeaf, leaf: "JSON"}
		}
		out := &node{kind: kObject, fields: map[string]*node{}, order: n.order}
		for _, k := range n.order {
			out.fields[k] = cutInner(n.fields[k], join(prefix, k), depth+1, maxDepth, depths)
		}
		return out
	default:
		return n
	}
}

func overrideBelow(prefix string, depths map[string]int) bool {
	for k := range depths {
		if k != "" && (prefix == "" || strings.HasPrefix(k, prefix+".")) {
			return true
		}
	}
	return false
}

func join(prefix, k string) string {
	if prefix == "" {
		return k
	}
	return prefix + "." + k
}

func (n *node) String() string {
	b, _ := json.Marshal(n.toAny())
	return string(b)
}

func (n *node) toAny() any {
	switch n.kind {
	case kLeaf:
		if n.leaf == "NULL" {
			return "JSON"
		}
		return n.leaf
	case kArray:
		return []any{n.elem.toAny()}
	default:
		m := map[string]any{}
		for _, k := range n.order {
			m[k] = n.fields[k].toAny()
		}
		return m
	}
}

func (n *node) at(path []string) *node {
	cur := n
	for _, s := range path {
		if cur == nil || cur.kind != kObject {
			return nil
		}
		cur = cur.fields[s]
	}
	return cur
}

// ---------------------------------------------------------------------------
// flatten() -> leaf projections
// ---------------------------------------------------------------------------

var flattenRe = regexp.MustCompile(`(?i)\bflatten\(\s*([^,\)]+?)\s*(?:,\s*(\d+))?\s*\)`)

// reformat mirrors olake utils.Reformat: lowercase, every non-letter/digit rune becomes '_'.
func reformat(key string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(key) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	return b.String()
}

func mangle(path []string) string {
	s := reformat(strings.Join(path, "_"))
	if s == "" {
		s = "_"
	}
	return s
}

func ref(path []string) string {
	var parts []string
	for _, p := range path {
		parts = append(parts, `"`+strings.ReplaceAll(p, `"`, `""`)+`"`)
	}
	return strings.Join(parts, ".")
}

type term struct {
	raw   string
	path  []string // nil = root
	depth int
}

func parseTerms(q string, defaultDepth int, docCol string) []term {
	var out []term
	for _, m := range flattenRe.FindAllStringSubmatch(q, -1) {
		t := term{raw: m[0], depth: defaultDepth}
		if p := strings.TrimSpace(m[1]); p != "*" && p != "$" {
			t.path = strings.Split(p, ".")
			if strings.EqualFold(t.path[0], "_olake_raw") {
				t.path = t.path[1:] // flatten(_olake_raw, N) == flatten(*, N)
			}
		}
		if m[2] != "" {
			t.depth, _ = strconv.Atoi(m[2])
		}
		out = append(out, t)
	}
	return out
}

type leafCol struct {
	path   []string
	asJSON bool // object left open by another term but past THIS term's depth: emit as JSON text
}

// leaves lists the columns a term produces: every leaf of the CUT structure under
// path, descending at most depth levels below it. Objects that are still open at
// the limit (an ancestor of a deeper flatten() term) are emitted as JSON text, so
// overlapping terms never leak each other's columns.
func leaves(n *node, path []string, depth, maxDepth int, out *[]leafCol) {
	if n.kind == kObject {
		if depth >= maxDepth {
			*out = append(*out, leafCol{path: path, asJSON: true})
			return
		}
		for _, k := range n.order {
			leaves(n.fields[k], append(append([]string{}, path...), k), depth+1, maxDepth, out)
		}
		return
	}
	*out = append(*out, leafCol{path: path})
}

// rewrite replaces every flatten() term with its leaf projections. Returns the
// SQL and the alias->expression map (for collision reporting).
func rewrite(q string, shaped *node, terms []term, docCol string) (string, []string) {
	owner := map[string]string{}
	var collisions []string
	i := 0
	out := flattenRe.ReplaceAllStringFunc(q, func(string) string {
		t := terms[i]
		i++
		root := shaped.at(t.path)
		if root == nil {
			return fmt.Sprintf("/* %s: path not seen yet */ NULL AS %s", t.raw, mangle(t.path))
		}
		var ls []leafCol
		if root.kind == kObject {
			leaves(root, t.path, 0, t.depth, &ls)
		} else {
			ls = []leafCol{{path: t.path}}
		}
		parts := make([]string, 0, len(ls))
		for _, lc := range ls {
			p := lc.path
			a, e := mangle(p), ref(p)
			if lc.asJSON {
				e = "to_json(" + e + ")"
			}
			if len(p) == 0 { // non-object root: the raw document itself
				a, e = "_olake_raw", "_olake_raw"
			}
			if prev, dup := owner[a]; dup {
				if prev != e {
					collisions = append(collisions, fmt.Sprintf("%s <- %s AND %s", a, prev, e))
				}
				continue // same column requested twice: emit once
			}
			owner[a] = e
			parts = append(parts, e+" AS "+a)
		}
		return strings.Join(parts, ",\n       ")
	})
	return out, collisions
}

// ---------------------------------------------------------------------------
// pipeline
// ---------------------------------------------------------------------------

type pipeline struct {
	db       *sql.DB
	running  *node
	depth    int
	query    string
	docCol   string // name of the JSON column; "" = whole row is the document
	out      string // parquet output path ("" = none); batches get a _bN suffix
	verbose  bool   // print structures + generated SQL
	shred    bool   // shred variant columns using the running structure (§4.6 policy, simplified)
	batchNo  int
	prevCols []string
	lastSQL  string
}

func (p *pipeline) runBatch(ctx context.Context, docExpr, from string, preview int) error {
	var raw string
	if err := p.db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT json_group_structure(%s)::VARCHAR FROM %s b", docExpr, from)).Scan(&raw); err != nil {
		return err
	}
	var parsed any
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return err
	}
	same := func(path []string, want string) bool {
		jp := "$"
		for _, seg := range path {
			jp += `."` + strings.ReplaceAll(seg, `"`, `\"`) + `"`
		}
		var bad int
		err := p.db.QueryRowContext(ctx, fmt.Sprintf(
			"SELECT count(*) FROM %s b WHERE json_type(%s, '%s') IS NOT NULL AND json_type(%s, '%s') <> '%s'",
			from, docExpr, jp, docExpr, jp, want)).Scan(&bad)
		return err == nil && bad == 0
	}
	p.running = merge(p.running, parseStructure(parsed), nil, same)

	terms := parseTerms(p.query, p.depth, p.docCol)
	depths := map[string]int{"": p.depth}
	for _, t := range terms {
		depths[strings.Join(t.path, ".")] = t.depth
	}
	shaped := cut(p.running, "", 0, depths)

	var inner string
	if shaped.kind == kObject {
		inner = fmt.Sprintf("SELECT b.*, json_transform(%s, '%s') AS __o FROM %s b",
			docExpr, strings.ReplaceAll(shaped.String(), "'", "''"), from)
	} else { // non-object root: nothing to transform
		inner = fmt.Sprintf("SELECT b.*, %s::VARCHAR AS __o FROM %s b", docExpr, from)
	}
	user, collisions := rewrite(p.query, shaped, terms, p.docCol)
	// the raw document stays reachable as _olake_raw (for ::VARIANT, overflow, audit)
	expose := `SELECT i.* EXCLUDE (__o, doc), doc AS _olake_raw, __o.* FROM __inner i`
	if shaped.kind != kObject { // non-object root: nothing to unpack
		expose = `SELECT i.* EXCLUDE (__o, doc), __o AS _olake_raw FROM __inner i`
	}
	q := fmt.Sprintf("WITH __inner AS (%s),\nbatch AS (%s)\n%s", inner, expose, user)

	if p.verbose {
		fmt.Printf("  batch structure : %s\n", raw)
		fmt.Printf("  running (merged): %s\n", p.running)
		if q != p.lastSQL {
			fmt.Printf("  after depth cut : %s\n", shaped)
			fmt.Printf("  generated SQL   :\n%s\n", indent(q))
		} else {
			fmt.Println("  SQL unchanged -> cached plan")
		}
	}
	p.lastSQL = q
	if len(collisions) > 0 {
		return fmt.Errorf("column name collisions (fail at check): %s", strings.Join(collisions, "; "))
	}
	p.batchNo++

	// output schema via DESCRIBE (works for every type, including VARIANT which Arrow export may not carry)
	rows, err := p.db.QueryContext(ctx, "DESCRIBE ("+q+")")
	if err != nil {
		return err
	}
	var outCols, outTypes []string
	for rows.Next() {
		var name, typ string
		var x1, x2, x3, x4 any
		if err := rows.Scan(&name, &typ, &x1, &x2, &x3, &x4); err != nil {
			rows.Close()
			return err
		}
		outCols = append(outCols, name)
		outTypes = append(outTypes, typ)
	}
	rows.Close()
	names := make([]string, len(outCols))
	for i := range outCols {
		names[i] = reformat(outCols[i])
		outTypes[i] = strings.ToLower(outTypes[i])
	}

	// ::VARIANT is a marker: DESCRIBE saw it (schema source); execution gets JSON text
	// and Go builds the Arrow variant column (§4.5). ::JSON works for VARCHAR and STRUCT input.
	isVariant := make([]bool, len(outCols))
	for i, t := range outTypes {
		isVariant[i] = t == "variant"
	}
	qExec := variantRe.ReplaceAllString(q, "::JSON")

	target := ""
	if p.out != "" {
		target = p.out
		if p.batchNo > 1 {
			target = strings.TrimSuffix(p.out, ".parquet") + fmt.Sprintf("_b%d.parquet", p.batchNo)
		}
	}
	var shredType arrow.DataType
	if p.shred {
		shredType = shreddingSchema(p.running, 300)
		if p.verbose {
			fmt.Printf("  shredding schema: %v\n", shredType)
		}
	}
	rows2, total, err := p.runArrow(ctx, qExec, names, isVariant, shredType, target, preview)
	if err != nil {
		if p.verbose {
			fmt.Printf("  arrow path failed (%v); falling back to database/sql preview, no parquet\n", err)
		}
		rows2, total, err = p.fetchRows(ctx, qExec, preview)
		if err != nil {
			return err
		}
	} else if target != "" {
		fmt.Printf("  parquet         : %s\n", target)
	}
	renderTable(names, outTypes, rows2, total)

	if p.prevCols != nil {
		if added := diff(names, p.prevCols); len(added) > 0 {
			fmt.Printf("  schema evolution: +%v\n", added)
		}
		if gone := diff(p.prevCols, names); len(gone) > 0 {
			fmt.Printf("  !! columns disappeared: %v\n", gone)
		}
	}
	p.prevCols = names
	return nil
}

var variantRe = regexp.MustCompile(`(?i)::\s*VARIANT\b`)

// runArrow executes q through DuckDB's Arrow export, rebuilds every record with
// OLake-style names, PARQUET:field_id metadata and real Arrow VARIANT columns,
// optionally writes them to Parquet with pqarrow (no DuckDB involved), and
// returns a preview. This is the shape the arrow-iceberg writer would receive.
func (p *pipeline) runArrow(ctx context.Context, q string, names []string, isVariant []bool,
	shredType arrow.DataType, target string, limit int) ([][]string, int, error) {
	mem := memory.DefaultAllocator
	conn, err := p.db.Conn(ctx)
	if err != nil {
		return nil, 0, err
	}
	defer conn.Close()

	var preview [][]string
	total := 0
	var w *pqarrow.FileWriter
	err = conn.Raw(func(dc any) error {
		ar, err := duckdb.NewArrowFromConn(dc.(driver.Conn))
		if err != nil {
			return err
		}
		rdr, err := ar.QueryContext(ctx, q)
		if err != nil {
			return err
		}
		defer rdr.Release()
		for rdr.Next() {
			rec, err := rebuild(mem, rdr.Record(), names, isVariant, shredType)
			if err != nil {
				return err
			}
			if target != "" {
				if w == nil {
					f, err := os.Create(target)
					if err != nil {
						rec.Release()
						return err
					}
					w, err = pqarrow.NewFileWriter(rec.Schema(), f,
						parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Zstd)),
						pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()))
					if err != nil {
						rec.Release()
						return err
					}
				}
				if err := w.Write(rec); err != nil {
					rec.Release()
					return err
				}
			}
			for r := 0; r < int(rec.NumRows()); r++ {
				if total < limit {
					cells := make([]string, rec.NumCols())
					for i := 0; i < int(rec.NumCols()); i++ {
						if rec.Column(i).IsNull(r) {
							cells[i] = "NULL"
						} else {
							cells[i] = strings.Clone(rec.Column(i).ValueStr(r))
						}
					}
					preview = append(preview, cells)
				}
				total++
			}
			rec.Release()
		}
		return rdr.Err()
	})
	if w != nil {
		if cerr := w.Close(); err == nil {
			err = cerr
		}
	}
	return preview, total, err
}

// rebuild renames columns, adds field ids and converts flagged JSON-text columns
// into Arrow VARIANT extension arrays (shredded if shredType != nil).
func rebuild(mem memory.Allocator, in arrow.Record, names []string, isVariant []bool, shredType arrow.DataType) (arrow.Record, error) {
	fields := make([]arrow.Field, in.NumCols())
	cols := make([]arrow.Array, in.NumCols())
	for i := 0; i < int(in.NumCols()); i++ {
		f := in.Schema().Field(i)
		col := in.Column(i)
		if i < len(isVariant) && isVariant[i] {
			vt := extensions.NewDefaultVariantType()
			if shredType != nil {
				vt = extensions.NewShreddedVariantType(shredType)
			}
			vb := extensions.NewVariantBuilder(mem, vt)
			for r := 0; r < col.Len(); r++ {
				if col.IsNull(r) {
					vb.AppendNull()
					continue
				}
				v, err := variant.ParseJSON(col.ValueStr(r), false)
				if err != nil {
					vb.Release()
					return nil, fmt.Errorf("column %s row %d: %w", f.Name, r, err)
				}
				vb.Append(v)
			}
			col = vb.NewArray()
			vb.Release()
			f.Type = vt
		} else {
			col.Retain()
		}
		name := f.Name
		if i < len(names) {
			name = names[i]
		}
		fields[i] = arrow.Field{Name: name, Type: f.Type, Nullable: true,
			Metadata: arrow.MetadataFrom(map[string]string{"PARQUET:field_id": fmt.Sprint(i + 1)})}
		cols[i] = col
	}
	rec := array.NewRecord(arrow.NewSchema(fields, nil), cols, in.NumRows())
	for _, c := range cols {
		c.Release()
	}
	return rec, nil
}

// shreddingSchema derives the typed_value schema from the running structure:
// objects recurse, primitive leaves with a stable type become typed, JSON/NULL
// leaves and arrays stay in the residual value. Leaf count capped.
func shreddingSchema(n *node, cap int) arrow.DataType {
	count := 0
	var rec func(n *node) arrow.DataType
	rec = func(n *node) arrow.DataType {
		if n == nil || n.kind != kObject {
			return nil
		}
		var fields []arrow.Field
		for _, k := range n.order {
			if count >= cap {
				break
			}
			c := n.fields[k]
			switch c.kind {
			case kObject:
				if t := rec(c); t != nil {
					fields = append(fields, arrow.Field{Name: k, Type: t, Nullable: true})
				}
			case kLeaf:
				var t arrow.DataType
				switch c.leaf {
				case "UBIGINT", "BIGINT", "HUGEINT", "UHUGEINT":
					t = arrow.PrimitiveTypes.Int64
				case "DOUBLE":
					t = arrow.PrimitiveTypes.Float64
				case "VARCHAR":
					t = arrow.BinaryTypes.String
				case "BOOLEAN":
					t = arrow.FixedWidthTypes.Boolean
				}
				if t != nil {
					fields = append(fields, arrow.Field{Name: k, Type: t, Nullable: true})
					count++
				}
			}
		}
		if len(fields) == 0 {
			return nil
		}
		return arrow.StructOf(fields...)
	}
	return rec(n)
}

// fetchRows returns up to limit rows as strings plus the total row count.
// Arrow first (zero-copy path OLake would use); database/sql as fallback for
// types the bundled DuckDB cannot export as Arrow (e.g. VARIANT).
func (p *pipeline) fetchRows(ctx context.Context, q string, limit int) ([][]string, int, error) {
	var out [][]string
	total := 0
	conn, err := p.db.Conn(ctx)
	if err != nil {
		return nil, 0, err
	}
	defer conn.Close()
	arrowErr := conn.Raw(func(dc any) error {
		ar, err := duckdb.NewArrowFromConn(dc.(driver.Conn))
		if err != nil {
			return err
		}
		rdr, err := ar.QueryContext(ctx, q)
		if err != nil {
			return err
		}
		defer rdr.Release()
		for rdr.Next() {
			rec := rdr.Record()
			for r := 0; r < int(rec.NumRows()); r++ {
				if total < limit {
					cells := make([]string, rec.NumCols())
					for i := 0; i < int(rec.NumCols()); i++ {
						if rec.Column(i).IsNull(r) {
							cells[i] = "NULL"
						} else {
							cells[i] = strings.Clone(rec.Column(i).ValueStr(r)) // ValueStr aliases the Arrow buffer
						}
					}
					out = append(out, cells)
				}
				total++
			}
		}
		return rdr.Err()
	})
	if arrowErr == nil {
		return out, total, nil
	}
	if p.verbose {
		fmt.Printf("  arrow export unavailable (%v); using database/sql\n", arrowErr)
	}
	out, total = nil, 0
	// database/sql cannot scan VARIANT and friends: stringify every column for the preview
	descr, err := p.db.QueryContext(ctx, "DESCRIBE ("+q+")")
	if err != nil {
		return nil, 0, err
	}
	var sel []string
	for descr.Next() {
		var name, typ string
		var x1, x2, x3, x4 any
		if err := descr.Scan(&name, &typ, &x1, &x2, &x3, &x4); err != nil {
			descr.Close()
			return nil, 0, err
		}
		qn := `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
		sel = append(sel, qn+"::VARCHAR AS "+qn)
	}
	descr.Close()
	rows, err := p.db.QueryContext(ctx, "SELECT "+strings.Join(sel, ", ")+" FROM ("+q+") __p")
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, 0, err
		}
		if total < limit {
			cells := make([]string, len(cols))
			for i, v := range vals {
				if v == nil {
					cells[i] = "NULL"
				} else {
					cells[i] = fmt.Sprint(v)
				}
			}
			out = append(out, cells)
		}
		total++
	}
	return out, total, rows.Err()
}

// renderTable prints a DuckDB-style box table: header, type row, rows.
func renderTable(names, types []string, rows [][]string, total int) {
	const maxCell = 48
	clip := func(s string) string {
		s = strings.ReplaceAll(strings.ReplaceAll(s, "\n", "\\n"), "\t", "\\t")
		if dispWidth(s) > maxCell {
			r := []rune(s)
			for dispWidth(string(r)) > maxCell-1 {
				r = r[:len(r)-1]
			}
			return string(r) + "…"
		}
		return s
	}
	n := len(names)
	widths := make([]int, n)
	for i := range names {
		widths[i] = max(dispWidth(names[i]), dispWidth(types[i]))
	}
	clipped := make([][]string, len(rows))
	for r, row := range rows {
		clipped[r] = make([]string, n)
		for i := 0; i < n && i < len(row); i++ {
			clipped[r][i] = clip(row[i])
			widths[i] = max(widths[i], dispWidth(clipped[r][i]))
		}
	}
	line := func(l, m, r string) string {
		parts := make([]string, n)
		for i := range parts {
			parts[i] = strings.Repeat("─", widths[i]+2)
		}
		return l + strings.Join(parts, m) + r
	}
	cellRow := func(cells []string) string {
		parts := make([]string, n)
		for i := range parts {
			c := ""
			if i < len(cells) {
				c = cells[i]
			}
			parts[i] = " " + c + strings.Repeat(" ", widths[i]-dispWidth(c)) + " "
		}
		return "│" + strings.Join(parts, "│") + "│"
	}
	fmt.Println(line("┌", "┬", "┐"))
	fmt.Println(cellRow(names))
	fmt.Println(cellRow(types))
	fmt.Println(line("├", "┼", "┤"))
	for _, row := range clipped {
		fmt.Println(cellRow(row))
	}
	fmt.Println(line("└", "┴", "┘"))
	if total > len(rows) {
		fmt.Printf("  %d rows (%d shown)\n", total, len(rows))
	} else {
		fmt.Printf("  %d rows\n", total)
	}
}

func dispWidth(s string) int {
	w := 0
	for _, r := range s {
		switch {
		case unicode.Is(unicode.Han, r), unicode.Is(unicode.Hangul, r), unicode.Is(unicode.Hiragana, r), unicode.Is(unicode.Katakana, r):
			w += 2
		default:
			w++
		}
	}
	return w
}

var _ = array.Struct{} // keep arrow imports honest if the walker is removed later
var _ arrow.Array

func indent(s string) string {
	return "    " + strings.ReplaceAll(s, "\n", "\n    ")
}

func diff(a, b []string) []string {
	seen := map[string]bool{}
	for _, x := range b {
		seen[x] = true
	}
	var out []string
	for _, x := range a {
		if !seen[x] {
			out = append(out, x)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// built-in edge cases (query defaults to: select flatten(*, depth) from batch)
// ---------------------------------------------------------------------------

type testCase struct {
	name    string
	depth   int
	query   string
	batches [][]string
	expect  string
}

const all = "select flatten(*) from batch"

var cases = []testCase{
	{"01 flat scalars", 3, all, [][]string{{
		`{"id":1,"name":"ada","ok":true,"score":9.5}`, `{"id":2,"name":"bob","ok":false,"score":1}`}},
		"score widens UBIGINT+DOUBLE -> DOUBLE"},
	{"02 default depth cut", 2, all, [][]string{{`{"a":{"b":{"c":{"d":1}},"x":1}}`}},
		"a_b is JSON text, a_x typed"},
	{"03 per-path override", 1,
		"select flatten(user, 2), flatten(meta.deep, 5), flatten(other) from batch", [][]string{{
			`{"user":{"id":7,"addr":{"city":"pune","geo":{"lat":1.5}}},"meta":{"deep":{"x":{"y":{"z":1}}}},"other":{"k":1}}`}},
		"user_addr_city + user_addr_geo(JSON); meta_deep_x_y_z; other(JSON at default 1)"},
	{"04 sparse keys", 3, all, [][]string{{`{"id":1,"a":{"b":1}}`, `{"id":2}`, `{"id":3,"a":{"c":"x"}}`}},
		"a_b and a_c both columns; missing -> null"},
	{"05 null then typed", 3, all, [][]string{{`{"k":null}`}, {`{"k":42}`}},
		"unknown -> UBIGINT, no JSON poisoning"},
	{"06 int then string", 3, all, [][]string{{`{"id":1}`}, {`{"id":"abc"}`}},
		"UBIGINT -> JSON, column kept"},
	{"07 int then float", 3, all, [][]string{{`{"v":1}`}, {`{"v":1.5}`}}, "UBIGINT -> DOUBLE"},
	{"08 huge ints", 3, all, [][]string{{`{"v":-1}`, `{"v":18446744073709551615}`, `{"v":1e30}`}},
		"DuckDB picks DOUBLE -> precision loss on 2^64-1 (OLake ladder should choose decimal/string)"},
	{"09 bool vs number", 3, all, [][]string{{`{"f":true}`}, {`{"f":1}`}}, "-> JSON, no true->1"},
	{"10 object then scalar", 3, all, [][]string{{`{"a":{"b":1}}`}, {`{"a":"str"}`}},
		"a_b disappears, a appears: incompatible type change -> OLake should fail loudly"},
	{"11 arrays", 3, all, [][]string{{`{"tags":[1,2,3],"names":["x","y"],"empty":[]}`, `{"tags":[],"names":[],"empty":[]}`}},
		"native lists; []-only -> list<JSON>"},
	{"12 array of objects", 3, all, [][]string{{`{"items":[{"sku":"a","qty":2},{"sku":"b","qty":1,"extra":true}]}`}},
		"list<struct>, keys unioned"},
	{"13 mixed array", 3, all, [][]string{{`{"arr":[1,"a",true,{"o":1}]}`}}, "list<JSON>"},
	{"14 nested arrays", 3, all, [][]string{{`{"m":[[1,2],[3]],"e":[[]]}`}}, "list<list<>>"},
	{"15 special key names", 3, all, [][]string{{
		`{"a.b":1,"a b":2,"a-b":3,"$oid":"x","名前":"n","123":4,"":5,"UPPER":6}`}},
		"mangled names collide -> refused"},
	{"16 flat-name collision", 3, all, [][]string{{`{"a":{"b":1},"a_b":2,"Id":1,"id":2}`}},
		"nested a.b vs literal a_b, Id vs id -> refused"},
	{"17 duplicate keys", 3, all, [][]string{{`{"a":1,"a":2}`}}, "parser: first wins"},
	{"18 typed-looking strings", 3, all, [][]string{{`{"amount":"12.50","ts":"2024-01-01T00:00:00Z","n":"007","b":"true"}`}},
		"all VARCHAR, no coercion"},
	{"19 deep cut at 6", 6, all, [][]string{{`{"l1":{"l2":{"l3":{"l4":{"l5":{"l6":{"l7":{"l8":1},"m":2}}}}}}}`}},
		"l1_l2_l3_l4_l5_l6 = JSON text of the rest"},
	{"20 non-object roots", 3, all, [][]string{{`"just a string"`, `42`, `[1,2]`}}, "raw passthrough"},
	{"21 empty objects", 3, all, [][]string{{`{}`, `{"e":{}}`}}, "e -> JSON leaf"},
	{"22 mongo extended json", 3, all, [][]string{{
		`{"_id":{"$oid":"66a1"},"ts":{"$date":"2024-01-01T00:00:00Z"},"n":{"$numberLong":"9"}}`}},
		"_id__oid etc; SMT can unwrap: select _id.\"$oid\" as id"},
	{"23 new nested key mid-stream", 3, all, [][]string{
		{`{"u":{"id":1}}`}, {`{"u":{"id":2,"addr":{"city":"goa"}}}`}, {`{"u":{"id":3}}`}},
		"+u_addr_city in batch 2, persists in batch 3, plan cached"},
	{"24 wide object", 3, all, [][]string{{wide(150)}}, "150 columns; policy guard needed"},
	{"25 unicode + escapes", 3, all, [][]string{{`{"s":"he said \"hi\" é \n tab\t","emoji":"🦆"}`}}, "untouched"},
	{"26 mixed: flatten + smt + passthrough", 3,
		`select flatten(customer, 2), lower(customer.email) as email_lc, status = 'paid' as is_paid, items from batch`,
		[][]string{{
			`{"customer":{"id":7,"email":"Ada@X.COM","addr":{"city":"pune","geo":{"lat":18.5}}},"items":[{"sku":"a","qty":2}],"status":"paid"}`}},
		"flat customer_* columns + derived + untouched list"},
}

func wide(n int) string {
	parts := make([]string, n)
	for i := range parts {
		parts[i] = fmt.Sprintf(`"k%03d":%d`, i, i)
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func runCases(ctx context.Context, db *sql.DB, preview int, only string, verbose bool) {
	for _, tc := range cases {
		if only != "" && !strings.Contains(tc.name, only) {
			continue
		}
		fmt.Printf("\n━━━ %s ━━━  depth=%d\n  query : %s\n  expect: %s\n", tc.name, tc.depth, tc.query, tc.expect)
		p := &pipeline{db: db, depth: tc.depth, query: tc.query, docCol: "doc", verbose: verbose}
		for bi, rows := range tc.batches {
			vals := make([]string, len(rows))
			for i, r := range rows {
				vals[i] = "('" + strings.ReplaceAll(r, "'", "''") + "')"
			}
			must(exec(ctx, db, "CREATE OR REPLACE TABLE __batch AS SELECT * FROM (VALUES "+strings.Join(vals, ",")+") t(doc)"))
			if len(tc.batches) > 1 {
				fmt.Printf("  -- batch %d --\n", bi+1)
			}
			if err := p.runBatch(ctx, "b.doc::JSON", "(SELECT * EXCLUDE (doc), doc FROM __batch)", preview); err != nil {
				fmt.Printf("  ERROR: %v\n", err)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

func main() {
	runBuiltin := flag.Bool("cases", false, "run the built-in edge cases")
	only := flag.String("only", "", "substring filter for -cases")
	src := flag.String("source", "", "input .jsonl: one JSON document per line")
	depth := flag.Int("depth", 3, "default depth for flatten() without an explicit one")
	query := flag.String("query", all, "user transform query; use flatten(path[, depth]) and FROM batch")
	batchSize := flag.Int("batch-size", 0, "rows per batch (0 = all)")
	preview := flag.Int("preview", 5, "rows to print per batch")
	out := flag.String("out", "", "write each batch's result to this parquet path (OLake-style column names)")
	verbose := flag.Bool("v", false, "print inferred structures and generated SQL")
	shred := flag.Bool("shred", false, "shred ::VARIANT columns (typed_value for stable primitive paths in the running structure)")
	flag.Parse()

	ctx := context.Background()
	db, err := sql.Open("duckdb", "")
	must(err)
	defer db.Close()
	var ver string
	must(db.QueryRowContext(ctx, "SELECT version()").Scan(&ver))
	if *verbose {
		fmt.Printf("duckdb (bundled by go-duckdb): %s\n", ver)
	}

	if *runBuiltin {
		runCases(ctx, db, *preview, *only, *verbose)
		return
	}
	if *src == "" {
		fmt.Fprintln(os.Stderr, "need -cases or -source")
		os.Exit(2)
	}

	// one document per line, kept as text: DuckDB must not infer a file-level schema here
	must(exec(ctx, db, fmt.Sprintf(
		"CREATE VIEW __src AS SELECT row_number() OVER () AS __rn, json::VARCHAR AS doc FROM read_json('%s', records = false, format = 'newline_delimited', columns = {json: 'JSON'})", *src)))
	docExpr := "b.doc::JSON"
	from := "__batch"

	var total int
	must(db.QueryRowContext(ctx, "SELECT count(*) FROM __src").Scan(&total))
	size := *batchSize
	if size <= 0 {
		size = total
	}
	p := &pipeline{db: db, depth: *depth, query: *query, docCol: "doc", out: *out, verbose: *verbose, shred: *shred}
	for off, n := 0, 1; off < total; off, n = off+size, n+1 {
		must(exec(ctx, db, fmt.Sprintf(
			"CREATE OR REPLACE VIEW __batch AS SELECT doc FROM __src WHERE __rn > %d AND __rn <= %d", off, off+size)))
		fmt.Printf("\n━━━ batch %d  rows %d..%d ━━━\n", n, off+1, min(off+size, total))
		must(p.runBatch(ctx, docExpr, from, *preview))
	}
}

func exec(ctx context.Context, db *sql.DB, q string) error {
	_, err := db.ExecContext(ctx, q)
	return err
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
