// Package arrowwriter — iceberg_backend.go is the per-thread iceberg-go
// integration that replaces the Java sink for the arrow-writer code path.
//
// One Backend instance is owned by exactly one ArrowWriter (one thread).
// It is responsible for:
//   - Building an AWS config (or generic IO props) and an iceberg.Catalog
//     handle for the configured catalog type (Glue, REST, JDBC, Hive).
//   - Ensuring the destination namespace + Iceberg table exist, creating
//     them with a partition spec derived from the OLake stream's
//     partitionInfo when the table is missing.
//   - Holding the loaded *table.Table reference for this thread and a
//     iceio.WriteFileIO handle used to upload Parquet bytes directly to
//     object storage at <table_loc>/data/<partition_path>/<uuid>.parquet.
//   - Serializing the Iceberg schema (data, equality-delete, positional-
//     delete) to JSON so the arrow-writer can stamp it into the parquet
//     "iceberg.schema" key-value metadata, matching iceberg-java behavior.
//   - Accumulating iceberg.DataFile descriptors per fileType and, on
//     Close, committing them via tx.AddDataFiles + tx.NewRowDelta in a
//     single iceberg-go transaction (no Java involvement).
//
// The Backend deliberately mirrors the layout demonstrated in the
// iceberg-go testing example testing/glue_partition_all_types so the
// commit semantics — including positional-/equality-delete handling —
// match what query engines (Spark, Trino, Athena) expect.
package arrowwriter

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/parquet/metadata"
	awscfgpkg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/google/uuid"

	iceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	_ "github.com/apache/iceberg-go/catalog/glue"
	_ "github.com/apache/iceberg-go/catalog/rest"
	_ "github.com/apache/iceberg-go/catalog/sql"
	iceio "github.com/apache/iceberg-go/io"
	_ "github.com/apache/iceberg-go/io/gocloud"
	icetable "github.com/apache/iceberg-go/table"
	iceutils "github.com/apache/iceberg-go/utils"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination/iceberg/internal"
	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

// Catalog type constants accepted by BackendConfig.CatalogType. They
// mirror the values used by the parent iceberg package so existing
// configs round-trip without translation.
const (
	CatalogGlue = "glue"
	CatalogRest = "rest"
	CatalogJDBC = "jdbc"
	CatalogHive = "hive"
)

// BackendConfig is the subset of OLake's iceberg.Config that the
// arrow-writer's iceberg-go backend actually needs. It is defined here
// (rather than imported from the parent iceberg package) to avoid a
// circular import — the parent package already imports this one.
type BackendConfig struct {
	// Catalog
	CatalogType string // glue | rest | jdbc | hive
	CatalogName string

	// Storage / warehouse
	IcebergS3Path   string // warehouse root, e.g. s3://bucket/prefix/
	IcebergDatabase string

	// AWS / S3-compatible IO
	Region       string
	AccessKey    string
	SecretKey    string
	SessionToken string
	ProfileName  string
	S3Endpoint   string
	S3UseSSL     bool
	S3PathStyle  bool

	// Glue overrides
	UseGlueAdditionalConfig bool
	GlueAccessKey           string
	GlueSecretKey           string
	GlueRegion              string
	GlueEndpoint            string
	GlueCatalogID           string

	// REST catalog
	RestCatalogURL    string
	RestSigningName   string
	RestSigningRegion string
	RestSigningV4     bool
	RestToken         string
	RestOAuthURI      string
	RestAuthType      string
	RestScope         string
	RestCredential    string

	// JDBC catalog
	JDBCUrl      string
	JDBCUsername string
	JDBCPassword string

	// Hive catalog
	HiveURI         string
	HiveClients     int
	HiveSaslEnabled bool
}

// partitionStruct is a positional iceberg.StructLike used to feed a
// partition tuple into PartitionSpec.PartitionToPath.
type partitionStruct []any

func (p partitionStruct) Size() int            { return len(p) }
func (p partitionStruct) Get(pos int) any      { return p[pos] }
func (p partitionStruct) Set(pos int, val any) { p[pos] = val }

// Backend wraps iceberg-go state for one OLake ingestion thread.
type Backend struct {
	cfg          *BackendConfig
	stream       types.StreamInterface
	threadID     string
	upsertMode   bool
	identifierID int // Iceberg field ID of _olake_id (0 if no identifier)

	cat        catalog.Catalog
	tbl        *icetable.Table
	wfs        iceio.WriteFileIO
	tableIdent icetable.Identifier

	// Per-thread accumulated files (committed on Close).
	dataFiles      []iceberg.DataFile
	eqDeleteFiles  []iceberg.DataFile
	posDeleteFiles []iceberg.DataFile

	// Cached schema JSONs, keyed by fileType (data | equalityDelete | positionalDelete).
	schemaJSONs map[string]string
	// columnTypeMap mirrors what the legacy Java path returned: column
	// name -> OLake iceberg type string (boolean, int, long, ...).
	columnTypeMap map[string]string

	// Partition field IDs in spec order. partitionValues[i] in the
	// arrow-writer corresponds to partitionFieldIDs[i] for iceberg-go.
	partitionFieldIDs []int

	// Partition info kept for transform-aware value coercion at commit time.
	partitionInfo []internal.PartitionInfo
}

// NewBackend bootstraps the per-thread iceberg-go state.
//
// protoSchema is the OLake-derived schema list (Key + IceType). On the
// first thread (when the table does not yet exist) Backend will create
// the table from this schema; on subsequent threads it loads the table
// from the catalog and ignores protoSchema.
func NewBackend(
	ctx context.Context,
	cfg *BackendConfig,
	partitionInfo []internal.PartitionInfo,
	stream types.StreamInterface,
	threadID string,
	upsertMode bool,
	identifierField string,
	protoSchema []*proto.IcebergPayload_SchemaField,
) (*Backend, error) {
	b := &Backend{
		cfg:           cfg,
		stream:        stream,
		threadID:      threadID,
		upsertMode:    upsertMode,
		partitionInfo: partitionInfo,
		schemaJSONs:   make(map[string]string),
	}

	// Inject AWS config into context — required by Glue/S3 IO when the
	// caller did not set the AWS_* env vars.
	ctx, err := b.injectAWSConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("backend: aws config: %w", err)
	}

	// Load the catalog.
	cat, err := b.loadCatalog(ctx)
	if err != nil {
		return nil, fmt.Errorf("backend: load catalog: %w", err)
	}
	b.cat = cat

	// Ensure namespace + table exist.
	tbl, err := b.ensureNamespaceAndTable(ctx, identifierField, protoSchema)
	if err != nil {
		return nil, fmt.Errorf("backend: ensure table: %w", err)
	}
	b.tbl = tbl

	// Load IO at the table location.
	if err := b.loadIO(ctx); err != nil {
		return nil, fmt.Errorf("backend: load IO: %w", err)
	}

	// Cache schema JSONs and the column->type map for the writer.
	if err := b.cacheSchemas(); err != nil {
		return nil, fmt.Errorf("backend: cache schemas: %w", err)
	}

	logger.Infof("Thread[%s]: iceberg-go backend ready (table=%s, location=%s)", threadID, b.tableIdent, tbl.Location())
	return b, nil
}

// ColumnTypeMap returns the OLake-style column->iceberg-type map
// derived from the loaded iceberg-go table schema. This replaces the
// schema map the Java server used to return.
func (b *Backend) ColumnTypeMap() map[string]string {
	out := make(map[string]string, len(b.columnTypeMap))
	for k, v := range b.columnTypeMap {
		out[k] = v
	}
	return out
}

// SchemaJSON returns the cached iceberg.schema JSON for the given
// fileType ("data" | "equalityDelete" | "positionalDelete").
func (b *Backend) SchemaJSON(fileType string) string { return b.schemaJSONs[fileType] }

// PartitionFieldIDs returns the partition field IDs in spec order. The
// caller's []any partition tuple is positionally aligned with this slice.
func (b *Backend) PartitionFieldIDs() []int { return b.partitionFieldIDs }

// AllocateFilePath constructs the destination object key for a parquet
// file as <table_loc>/data/<partitionPath>/<uuid>-<fileType>.parquet.
// partitionPath may be empty for unpartitioned tables.
func (b *Backend) AllocateFilePath(partitionPath, fileType string) string {
	loc := strings.TrimRight(b.tbl.Location(), "/")
	parts := []string{loc, "data"}
	if partitionPath != "" {
		parts = append(parts, partitionPath)
	}
	parts = append(parts, fmt.Sprintf("%s-%s.parquet", uuid.NewString(), fileType))
	return strings.Join(parts, "/")
}

// PartitionPath converts an ordered partition-tuple ([]any from the
// transforms layer) into the iceberg-go partition path
// (e.g. "c_bool=true/c_int=10/...").
func (b *Backend) PartitionPath(orderedValues []any) string {
	spec := b.tbl.Spec()
	if spec.IsUnpartitioned() {
		return ""
	}
	rec := make(partitionStruct, len(orderedValues))
	for i, v := range orderedValues {
		rec[i] = b.coercePartitionValue(i, v)
	}
	return spec.PartitionToPath(rec, b.tbl.Schema())
}

// UploadFile writes the parquet bytes to object storage via the
// catalog-provided WriteFileIO. The path must be one returned by
// AllocateFilePath.
func (b *Backend) UploadFile(path string, data []byte) error {
	if err := b.wfs.WriteFile(path, data); err != nil {
		return fmt.Errorf("backend: upload %s: %w", path, err)
	}
	return nil
}

// AddDataFile records a written data parquet for later commit.
func (b *Backend) AddDataFile(filePath string, fileSize int64, pqMeta *metadata.FileMetaData, orderedPartitionValues []any) error {
	df, err := b.buildDataFile(filePath, fileSize, pqMeta, orderedPartitionValues, iceberg.EntryContentData, nil, nil)
	if err != nil {
		return err
	}
	b.dataFiles = append(b.dataFiles, df)
	return nil
}

// AddEqualityDeleteFile records a written equality-delete parquet for
// later commit. eqFieldIDs identifies the columns that form the
// equality predicate (typically [_olake_id-field-id]).
func (b *Backend) AddEqualityDeleteFile(filePath string, fileSize int64, pqMeta *metadata.FileMetaData, orderedPartitionValues []any, eqFieldIDs []int) error {
	df, err := b.buildDataFile(filePath, fileSize, pqMeta, orderedPartitionValues, iceberg.EntryContentEqDeletes, eqFieldIDs, nil)
	if err != nil {
		return err
	}
	b.eqDeleteFiles = append(b.eqDeleteFiles, df)
	return nil
}

// AddPositionalDeleteFile records a written positional-delete parquet
// for later commit.
func (b *Backend) AddPositionalDeleteFile(filePath string, fileSize int64, pqMeta *metadata.FileMetaData, orderedPartitionValues []any) error {
	// Positional-delete files override metrics so manifest bounds carry
	// the full file_path range — matches iceberg-java's writer.
	props := iceberg.Properties{}
	for k, v := range b.tbl.Properties() {
		props[k] = v
	}
	props[icetable.MetricsModeColumnConfPrefix+".file_path"] = "full"

	df, err := b.buildDataFile(filePath, fileSize, pqMeta, orderedPartitionValues, iceberg.EntryContentPosDeletes, nil, props)
	if err != nil {
		return err
	}
	b.posDeleteFiles = append(b.posDeleteFiles, df)
	return nil
}

// Commit flushes all accumulated data and delete files into the table
// in a single multi-snapshot transaction:
//  1. tx.AddDataFiles(...)            — append snapshot for data files
//  2. tx.NewRowDelta(...).Commit(...) — delete snapshot for eq+pos deletes
//
// Either step is skipped if no files of that kind were accumulated.
func (b *Backend) Commit(ctx context.Context) error {
	if len(b.dataFiles) == 0 && len(b.eqDeleteFiles) == 0 && len(b.posDeleteFiles) == 0 {
		return nil
	}

	ctx, err := b.injectAWSConfig(ctx)
	if err != nil {
		return fmt.Errorf("backend: commit aws config: %w", err)
	}

	tx := b.tbl.NewTransaction()
	if len(b.dataFiles) > 0 {
		if err := tx.AddDataFiles(ctx, b.dataFiles, nil, icetable.WithoutDuplicateCheck()); err != nil {
			return fmt.Errorf("backend: AddDataFiles: %w", err)
		}
	}

	if len(b.eqDeleteFiles) > 0 || len(b.posDeleteFiles) > 0 {
		rd := tx.NewRowDelta(nil)
		for _, df := range b.eqDeleteFiles {
			rd.AddDeletes(df)
		}
		for _, df := range b.posDeleteFiles {
			rd.AddDeletes(df)
		}
		if err := rd.Commit(ctx); err != nil {
			return fmt.Errorf("backend: RowDelta commit: %w", err)
		}
	}

	updated, err := tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("backend: transaction commit: %w", err)
	}
	b.tbl = updated

	// Reset state; a Backend is single-shot but we leave state clean in
	// case the writer is reused.
	b.dataFiles = nil
	b.eqDeleteFiles = nil
	b.posDeleteFiles = nil

	logger.Infof("Thread[%s]: committed iceberg-go transaction (table=%s, snapshot=%s)", b.threadID, b.tableIdent, snapshotIDFor(updated))
	return nil
}

// EvolveSchema applies an additive/promotion-only schema update to the
// table by issuing a tx.UpdateSchema commit. newSchema is the OLake-
// shaped column->type map.
func (b *Backend) EvolveSchema(ctx context.Context, newSchema map[string]string) error {
	ctx, err := b.injectAWSConfig(ctx)
	if err != nil {
		return fmt.Errorf("backend: evolve aws config: %w", err)
	}

	tx := b.tbl.NewTransaction()
	upd := tx.UpdateSchema(true, false)

	current := b.columnTypeMap
	// Sort for deterministic update order.
	names := make([]string, 0, len(newSchema))
	for n := range newSchema {
		names = append(names, n)
	}
	sort.Strings(names)

	for _, name := range names {
		newType := newSchema[name]
		if oldType, exists := current[name]; exists {
			if oldType == newType {
				continue
			}
			t, err := olakeIcebergTypeToIceberg(newType)
			if err != nil {
				return fmt.Errorf("evolve %s: %w", name, err)
			}
			upd = upd.UpdateColumn([]string{name}, icetable.ColumnUpdate{
				FieldType: iceberg.Optional[iceberg.Type]{Val: t, Valid: true},
			})
			continue
		}
		t, err := olakeIcebergTypeToIceberg(newType)
		if err != nil {
			return fmt.Errorf("evolve add %s: %w", name, err)
		}
		// New columns are always added optional with no default; the
		// identifier (_olake_id) was created required at table-create
		// time and is not added via EvolveSchema.
		upd = upd.AddColumn([]string{name}, t, "", false, nil)
	}

	if err := upd.Commit(); err != nil {
		return fmt.Errorf("backend: schema update commit: %w", err)
	}

	updated, err := tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("backend: schema tx commit: %w", err)
	}
	b.tbl = updated

	if err := b.cacheSchemas(); err != nil {
		return fmt.Errorf("backend: refresh schemas after evolve: %w", err)
	}
	return nil
}

// IcebergSchema returns the current Iceberg schema. Used by the writer
// to derive the Arrow schema with field IDs.
func (b *Backend) IcebergSchema() *iceberg.Schema { return b.tbl.Schema() }

// Spec returns the current partition spec.
func (b *Backend) Spec() iceberg.PartitionSpec { return b.tbl.Spec() }

// BloomFilterColumns returns the per-column bloom-filter toggle map that
// getDefaultWriterProps consumes. The map's value is the enabled flag
// passed to parquet.WithBloomFilterEnabledFor.
//
// Defaults:
//
//   - The identifier column (typically _olake_id) is enabled when the
//     table has one. Equality-delete, upsert and dedupe queries all
//     filter on _olake_id; without a bloom filter those scans fall back
//     to row-group min/max stats which, on an unsorted high-cardinality
//     column, almost never let the reader skip a row group.
//
// Overrides:
//
//   - Every table property matching iceberg's
//     `write.parquet.bloom-filter-enabled.column.<col>` key
//     (icetable.ParquetBloomFilterColumnEnabledKeyPrefix) is layered on
//     top. The value is parsed exactly the way iceberg-go does
//     (table/internal/parquet_files.go:281): only the case-insensitive
//     literal "true" enables the filter; any other value (including "1"
//     or "yes") disables it. Explicit user settings — including disabling
//     _olake_id — therefore win over the default.
func (b *Backend) BloomFilterColumns() map[string]bool {
	out := make(map[string]bool)
	if b.identifierID > 0 {
		out[constants.OlakeID] = true
	}

	prefix := icetable.ParquetBloomFilterColumnEnabledKeyPrefix + "."
	for key, val := range b.tbl.Properties() {
		col, ok := strings.CutPrefix(key, prefix)
		if !ok || col == "" {
			continue
		}
		out[col] = strings.EqualFold(strings.TrimSpace(val), "true")
	}
	return out
}

// ────────────────────────────────────────────────────────────────────
// Internals
// ────────────────────────────────────────────────────────────────────

func (b *Backend) injectAWSConfig(ctx context.Context) (context.Context, error) {
	// Only the Glue catalog needs the AWS config carried on the context
	// when olake later calls tbl.FS(ctx): glue.NewFromLocation captures
	// fsF=io.LoadFSFunc(nil, loc) which has no props of its own and
	// falls back to whatever AWS config is on the context at FS() time.
	//
	// For REST / JDBC / Hive catalogs we MUST NOT inject the user's
	// HMAC keys here. iceberg-go's gocloud S3 IO checks the context
	// first (io/gocloud/s3.go: createS3Bucket) and uses that aws.Config
	// in preference to the merged props — silently overriding the
	// vended credentials a REST catalog like Lakekeeper provides via
	// its LoadTable response. That manifests as repeated 403
	// SignatureDoesNotMatch on PutObject / CreateMultipartUpload even
	// after my earlier fix routed loadIO through tbl.FS(ctx).
	//
	// REST/JDBC/Hive resolve credentials from props (S3AccessKeyID,
	// S3SecretAccessKey, S3SessionToken, S3EndpointURL, S3Region),
	// which are populated by applyS3IOProps at catalog-load time and
	// then overlaid with vended creds from StorageCredentials inside
	// vendedCredentialRefresher.loadFS. Skipping ctx injection for
	// those catalogs is what lets the vended path actually take effect.
	if b.cfg.CatalogType != CatalogGlue {
		return ctx, nil
	}

	loadOpts := []func(*awscfgpkg.LoadOptions) error{}
	if b.cfg.Region != "" {
		loadOpts = append(loadOpts, awscfgpkg.WithRegion(b.cfg.Region))
	}
	if b.cfg.AccessKey != "" || b.cfg.SecretKey != "" || b.cfg.SessionToken != "" {
		loadOpts = append(loadOpts, awscfgpkg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(b.cfg.AccessKey, b.cfg.SecretKey, b.cfg.SessionToken),
		))
	}

	awsCfg, err := awscfgpkg.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return ctx, err
	}
	return iceutils.WithAwsConfig(ctx, &awsCfg), nil
}

func (b *Backend) loadCatalog(ctx context.Context) (catalog.Catalog, error) {
	props := iceberg.Properties{
		"warehouse": b.cfg.IcebergS3Path,
	}

	switch b.cfg.CatalogType {
	case CatalogGlue:
		props["type"] = "glue"
		if b.cfg.AccessKey != "" {
			props["glue.access-key-id"] = b.cfg.AccessKey
		}
		if b.cfg.SecretKey != "" {
			props["glue.secret-access-key"] = b.cfg.SecretKey
		}
		if b.cfg.Region != "" {
			props["glue.region"] = b.cfg.Region
		}
		if b.cfg.UseGlueAdditionalConfig {
			if b.cfg.GlueAccessKey != "" {
				props["glue.access-key-id"] = b.cfg.GlueAccessKey
			}
			if b.cfg.GlueSecretKey != "" {
				props["glue.secret-access-key"] = b.cfg.GlueSecretKey
			}
			if b.cfg.GlueRegion != "" {
				props["glue.region"] = b.cfg.GlueRegion
			}
			if b.cfg.GlueEndpoint != "" {
				props["glue.endpoint"] = b.cfg.GlueEndpoint
			}
			if b.cfg.GlueCatalogID != "" {
				props["glue.id"] = b.cfg.GlueCatalogID
			}
		}
		return catalog.Load(ctx, "glue", props)

	case CatalogRest:
		props["type"] = "rest"
		props["uri"] = b.cfg.RestCatalogURL
		if b.cfg.RestToken != "" {
			props["token"] = b.cfg.RestToken
		}
		if b.cfg.RestOAuthURI != "" {
			props["oauth2-server-uri"] = b.cfg.RestOAuthURI
		}
		if b.cfg.RestCredential != "" {
			props["credential"] = b.cfg.RestCredential
		}
		if b.cfg.RestScope != "" {
			props["scope"] = b.cfg.RestScope
		}
		if b.cfg.RestSigningName != "" {
			props["rest.signing-name"] = b.cfg.RestSigningName
		}
		if b.cfg.RestSigningRegion != "" {
			props["rest.signing-region"] = b.cfg.RestSigningRegion
		}
		if b.cfg.RestSigningV4 {
			props["rest.sigv4-enabled"] = "true"
		}
		// S3 IO settings are also applied to REST so the IO factory can
		// reach the warehouse bucket directly.
		b.applyS3IOProps(props)
		return catalog.Load(ctx, b.cfg.CatalogName, props)

	case CatalogJDBC:
		props["type"] = "sql"
		props["uri"] = b.cfg.JDBCUrl
		if b.cfg.JDBCUsername != "" {
			props["jdbc.user"] = b.cfg.JDBCUsername
		}
		if b.cfg.JDBCPassword != "" {
			props["jdbc.password"] = b.cfg.JDBCPassword
		}
		b.applyS3IOProps(props)
		return catalog.Load(ctx, b.cfg.CatalogName, props)

	case CatalogHive:
		return nil, fmt.Errorf("hive catalog is not supported by the iceberg-go arrow-writer backend yet")

	default:
		return nil, fmt.Errorf("unsupported catalog type %q", b.cfg.CatalogType)
	}
}

func (b *Backend) applyS3IOProps(props iceberg.Properties) {
	if b.cfg.Region != "" {
		props[iceio.S3Region] = b.cfg.Region
	}
	if b.cfg.AccessKey != "" {
		props[iceio.S3AccessKeyID] = b.cfg.AccessKey
	}
	if b.cfg.SecretKey != "" {
		props[iceio.S3SecretAccessKey] = b.cfg.SecretKey
	}
	if b.cfg.SessionToken != "" {
		props[iceio.S3SessionToken] = b.cfg.SessionToken
	}
	if b.cfg.S3Endpoint != "" {
		props[iceio.S3EndpointURL] = b.cfg.S3Endpoint
	}
}

func (b *Backend) loadIO(ctx context.Context) error {
	// Prefer the table's own IO. For REST catalogs that vend storage
	// credentials (e.g. Lakekeeper), tbl.FS(ctx) is backed by a
	// refresher that uses the catalog-vended access key, secret, and
	// session token — required for buckets that reject the user's HMAC
	// keys with SignatureDoesNotMatch (e.g. GCS S3-compat where only
	// short-lived vended HMAC pairs are authorised for the prefix).
	//
	// We fall back to constructing an IO from the user-supplied HMAC
	// keys + endpoint only if the catalog did not vend an IO or the
	// vended IO does not implement WriteFileIO. This preserves the
	// previous behaviour for catalogs that don't vend credentials
	// (Glue, JDBC, plain REST without credential vending).
	if fs, err := b.tbl.FS(ctx); err == nil && fs != nil {
		if wfs, ok := fs.(iceio.WriteFileIO); ok {
			b.wfs = wfs
			return nil
		}
		// Vended IO exists but is read-only — fall through to manual
		// IO so writes still work, while logging the surprise.
		logger.Warnf("Thread[%s]: catalog-vended IO does not support writes; falling back to user-supplied S3 credentials", b.threadID)
	}

	props := iceberg.Properties{}
	b.applyS3IOProps(props)

	fs, err := iceio.LoadFS(ctx, props, b.tbl.Location())
	if err != nil {
		return err
	}
	wfs, ok := fs.(iceio.WriteFileIO)
	if !ok {
		return fmt.Errorf("io implementation does not support writes for location %s", b.tbl.Location())
	}
	b.wfs = wfs
	return nil
}

func (b *Backend) ensureNamespaceAndTable(
	ctx context.Context,
	identifierField string,
	protoSchema []*proto.IcebergPayload_SchemaField,
) (*icetable.Table, error) {
	dbName := b.stream.GetDestinationDatabase(&b.cfg.IcebergDatabase)
	tblName := b.stream.GetDestinationTable()

	nsIdent := catalog.ToIdentifier(dbName)
	tblIdent := catalog.ToIdentifier(dbName, tblName)
	b.tableIdent = tblIdent

	// warehouseIsURI distinguishes a real storage URI (e.g.
	// "s3://bucket/prefix", "gs://bucket/prefix") from a logical
	// warehouse *name* used by REST catalogs like Lakekeeper (e.g.
	// "dz-s3-compatible"). When only a name is given, the REST catalog
	// resolves the actual storage location server-side and we MUST NOT
	// pass relative-path locations of our own — Lakekeeper rejects them
	// with "Failed to parse '<name>/<db>/<tbl>' as Location: Not a
	// valid URL — relative URL without a base". In that case let the
	// catalog derive namespace and table locations from its warehouse
	// config, mirroring testing/lakekeeper_vended/main.go.
	warehouseIsURI := strings.Contains(b.cfg.IcebergS3Path, "://")

	// Namespace
	nsExists, err := b.cat.CheckNamespaceExists(ctx, nsIdent)
	if err != nil {
		return nil, fmt.Errorf("check namespace %s: %w", dbName, err)
	}
	if !nsExists {
		var nsProps iceberg.Properties
		if warehouseIsURI {
			nsProps = iceberg.Properties{
				"location": strings.TrimRight(b.cfg.IcebergS3Path, "/") + "/" + dbName,
			}
		}
		if err := b.cat.CreateNamespace(ctx, nsIdent, nsProps); err != nil {
			// Tolerate races between concurrent threads racing for namespace creation.
			if !isAlreadyExistsErr(err) {
				return nil, fmt.Errorf("create namespace %s: %w", dbName, err)
			}
		}
	}

	// Table
	tblExists, err := b.cat.CheckTableExists(ctx, tblIdent)
	if err != nil {
		return nil, fmt.Errorf("check table %s.%s: %w", dbName, tblName, err)
	}
	if tblExists {
		tbl, err := b.cat.LoadTable(ctx, tblIdent)
		if err != nil {
			return nil, fmt.Errorf("load existing table %s.%s: %w", dbName, tblName, err)
		}
		// Recover partition field IDs from the loaded spec so writes
		// align with whatever the original creator chose.
		b.partitionFieldIDs = collectPartitionFieldIDs(tbl.Spec())
		// Identifier ID is needed for the equality-delete schema.
		b.identifierID = identifierFieldIDFromSchema(tbl.Schema(), identifierField)
		return tbl, nil
	}

	// Build schema + partition spec and create the table.
	sc, identID, err := b.buildIcebergSchema(protoSchema, identifierField)
	if err != nil {
		return nil, fmt.Errorf("build iceberg schema: %w", err)
	}
	b.identifierID = identID

	spec, err := b.buildPartitionSpec(sc)
	if err != nil {
		return nil, fmt.Errorf("build partition spec: %w", err)
	}
	b.partitionFieldIDs = collectPartitionFieldIDs(spec)

	createOpts := []catalog.CreateTableOpt{
		catalog.WithPartitionSpec(&spec),
		catalog.WithProperties(iceberg.Properties{
			"format-version":       "2",
			"write.format.default": "parquet",
			// Concurrent OLake threads each load the table at the same
			// base metadata and race to commit; iceberg-go's
			// commit-retry loop (refresh + replay) is what makes those
			// commits converge. The defaults are 0 retries which is
			// not viable for our multi-thread ingestion model.
			icetable.CommitNumRetriesKey:          "8",
			icetable.CommitMinRetryWaitMsKey:      "100",
			icetable.CommitMaxRetryWaitMsKey:      "5000",
			icetable.CommitTotalRetryTimeoutMsKey: "300000",
		}),
	}
	// Only assert an explicit table location when the warehouse is a
	// real storage URI. For warehouse-name configs (REST/Lakekeeper),
	// omitting WithLocation lets the catalog place the table under the
	// warehouse's server-managed root.
	if warehouseIsURI {
		tableLoc := strings.TrimRight(b.cfg.IcebergS3Path, "/") + "/" + dbName + "/" + tblName
		createOpts = append(createOpts, catalog.WithLocation(tableLoc))
	}
	tbl, err := b.cat.CreateTable(ctx, tblIdent, sc, createOpts...)
	if err != nil {
		// Another concurrent thread may have just created the table —
		// fall back to LoadTable instead of failing the whole sync.
		if isAlreadyExistsErr(err) {
			loaded, lerr := b.cat.LoadTable(ctx, tblIdent)
			if lerr != nil {
				return nil, fmt.Errorf("create table raced and reload failed: create=%v, load=%w", err, lerr)
			}
			b.partitionFieldIDs = collectPartitionFieldIDs(loaded.Spec())
			b.identifierID = identifierFieldIDFromSchema(loaded.Schema(), identifierField)
			return loaded, nil
		}
		return nil, fmt.Errorf("create table %s.%s: %w", dbName, tblName, err)
	}
	return tbl, nil
}

// buildIcebergSchema converts OLake's [Key,IceType] schema into an
// iceberg.Schema with stable, sequential field IDs (1, 2, 3, ...).
// Field order is alphabetical for determinism. The column whose name
// matches identifierField (typically _olake_id) is marked required.
func (b *Backend) buildIcebergSchema(
	protoSchema []*proto.IcebergPayload_SchemaField,
	identifierField string,
) (*iceberg.Schema, int, error) {
	if len(protoSchema) == 0 {
		return nil, 0, fmt.Errorf("schema is empty — cannot create iceberg table")
	}

	// Deduplicate by name, keep first (ToIceberg can theoretically emit
	// duplicates if a column is both an OLake column and a partition
	// column).
	seen := make(map[string]string, len(protoSchema))
	for _, f := range protoSchema {
		if _, ok := seen[f.Key]; !ok {
			seen[f.Key] = f.IceType
		}
	}

	names := make([]string, 0, len(seen))
	for n := range seen {
		names = append(names, n)
	}
	sort.Strings(names)

	fields := make([]iceberg.NestedField, 0, len(names))
	identifierID := 0
	for i, name := range names {
		t, err := olakeIcebergTypeToIceberg(seen[name])
		if err != nil {
			return nil, 0, fmt.Errorf("column %s: %w", name, err)
		}
		fid := i + 1
		required := false
		if identifierField != "" && name == identifierField {
			required = true
			identifierID = fid
		}
		fields = append(fields, iceberg.NestedField{
			ID:       fid,
			Name:     name,
			Type:     t,
			Required: required,
		})
	}
	if identifierID > 0 {
		return iceberg.NewSchemaWithIdentifiers(0, []int{identifierID}, fields...), identifierID, nil
	}
	return iceberg.NewSchema(0, fields...), identifierID, nil
}

// buildPartitionSpec turns OLake's []PartitionInfo into an
// iceberg.PartitionSpec. Each partition source must already exist in
// the schema (by SchemaField name).
func (b *Backend) buildPartitionSpec(sc *iceberg.Schema) (iceberg.PartitionSpec, error) {
	if len(b.partitionInfo) == 0 {
		return *iceberg.UnpartitionedSpec, nil
	}

	pfields := make([]iceberg.PartitionField, 0, len(b.partitionInfo))
	for i, pInfo := range b.partitionInfo {
		field, found := sc.FindFieldByName(pInfo.SchemaField)
		if !found {
			return iceberg.PartitionSpec{}, fmt.Errorf("partition source column %q missing from schema", pInfo.SchemaField)
		}
		t, err := parseIcebergTransformString(pInfo.Transform)
		if err != nil {
			return iceberg.PartitionSpec{}, fmt.Errorf("partition column %s: %w", pInfo.SchemaField, err)
		}
		pfields = append(pfields, iceberg.PartitionField{
			SourceIDs: []int{field.ID},
			FieldID:   iceberg.PartitionDataIDStart + i,
			Name:      partitionFieldName(pInfo),
			Transform: t,
		})
	}
	return iceberg.NewPartitionSpec(pfields...), nil
}

func (b *Backend) cacheSchemas() error {
	dataJSON, err := json.Marshal(b.tbl.Schema())
	if err != nil {
		return fmt.Errorf("marshal data schema: %w", err)
	}
	b.schemaJSONs[fileTypeData] = string(dataJSON)

	if b.upsertMode {
		if b.identifierID == 0 {
			return fmt.Errorf("upsert mode but no identifier field id resolved (set NoIdentifierFields=false)")
		}
		eqDelSchema := iceberg.NewSchema(0,
			iceberg.NestedField{
				ID: b.identifierID, Name: constants.OlakeID, Type: iceberg.PrimitiveTypes.String, Required: true,
			},
		)
		eqJSON, err := json.Marshal(eqDelSchema)
		if err != nil {
			return fmt.Errorf("marshal eq-delete schema: %w", err)
		}
		b.schemaJSONs[fileTypeEqualityDelete] = string(eqJSON)

		posJSON, err := json.Marshal(iceberg.PositionalDeleteSchema)
		if err != nil {
			return fmt.Errorf("marshal pos-delete schema: %w", err)
		}
		b.schemaJSONs[fileTypePositionalDelete] = string(posJSON)
	}

	b.columnTypeMap = icebergSchemaToColumnTypeMap(b.tbl.Schema())
	return nil
}

func (b *Backend) buildDataFile(
	filePath string,
	fileSize int64,
	pqMeta *metadata.FileMetaData,
	orderedPartitionValues []any,
	content iceberg.ManifestEntryContent,
	eqFieldIDs []int,
	overrideProps iceberg.Properties,
) (iceberg.DataFile, error) {
	partitionValues := b.partitionValuesByID(orderedPartitionValues)

	props := overrideProps
	if props == nil {
		props = b.tbl.Properties()
	}

	// For positional-delete files iceberg-java uses the pos-delete
	// schema when computing metrics; for eq-delete files it uses the
	// equality schema; for data files the table schema. Match here.
	fileSchema := b.tbl.Schema()
	switch content {
	case iceberg.EntryContentPosDeletes:
		fileSchema = iceberg.PositionalDeleteSchema
	case iceberg.EntryContentEqDeletes:
		// Build a single-column schema matching the equality delete
		// parquet (just _olake_id).
		fileSchema = iceberg.NewSchema(0,
			iceberg.NestedField{
				ID: b.identifierID, Name: constants.OlakeID, Type: iceberg.PrimitiveTypes.String, Required: true,
			},
		)
	}

	args := icetable.ParquetDataFileArgs{
		Schema:           fileSchema,
		Spec:             b.tbl.Spec(),
		ParquetMetadata:  pqMeta,
		FilePath:         filePath,
		FileSize:         fileSize,
		Content:          content,
		SortOrderID:      b.tbl.SortOrder().OrderID(),
		Properties:       props,
		PartitionValues:  partitionValues,
		EqualityFieldIDs: eqFieldIDs,
	}
	df, err := icetable.DataFileFromParquetMetadata(args)
	if err != nil {
		return nil, fmt.Errorf("build data file %s: %w", filePath, err)
	}
	return df, nil
}

// partitionValuesByID maps the writer's positional []any tuple to the
// {fieldID -> value} map that iceberg-go expects.
func (b *Backend) partitionValuesByID(orderedValues []any) map[int]any {
	if len(b.partitionFieldIDs) == 0 {
		return nil
	}
	out := make(map[int]any, len(b.partitionFieldIDs))
	for i, fid := range b.partitionFieldIDs {
		var v any
		if i < len(orderedValues) {
			v = b.coercePartitionValue(i, orderedValues[i])
		}
		out[fid] = v
	}
	return out
}

// coercePartitionValue normalises OLake-emitted partition values to the
// iceberg-go domain types. The arrow-writer's transform layer emits
// raw int64 microseconds for identity(timestamptz), but iceberg-go
// stores those as iceberg.Timestamp.
func (b *Backend) coercePartitionValue(idx int, v any) any {
	if v == nil || idx >= len(b.partitionInfo) {
		return v
	}
	pInfo := b.partitionInfo[idx]
	tlower := strings.ToLower(strings.TrimSpace(pInfo.Transform))
	if !strings.HasPrefix(tlower, "identity") {
		// year/month/day/hour already produce int32; bucket → int32;
		// truncate preserves source type which already matches.
		return v
	}
	// Identity transform — coerce timestamptz int64 to iceberg.Timestamp.
	if i64, ok := v.(int64); ok {
		// Look up the column type — if it's timestamptz this is microseconds.
		if colType, ok := b.columnTypeMap[pInfo.SchemaField]; ok && colType == "timestamptz" {
			return iceberg.Timestamp(i64)
		}
	}
	return v
}

// ────────────────────────────────────────────────────────────────────
// Free-standing helpers
// ────────────────────────────────────────────────────────────────────

func partitionFieldName(p internal.PartitionInfo) string {
	base, _, err := parseTransform(p.Transform)
	if err != nil || base == "" || base == "identity" {
		return p.SchemaField
	}
	switch base {
	case "bucket":
		return p.SchemaField + "_bucket"
	case "truncate":
		return p.SchemaField + "_trunc"
	default:
		return p.SchemaField + "_" + base
	}
}

func collectPartitionFieldIDs(spec iceberg.PartitionSpec) []int {
	out := make([]int, 0, spec.NumFields())
	for i := 0; i < spec.NumFields(); i++ {
		out = append(out, spec.Field(i).FieldID)
	}
	return out
}

func identifierFieldIDFromSchema(sc *iceberg.Schema, identifierField string) int {
	if identifierField == "" {
		return 0
	}
	if f, ok := sc.FindFieldByName(identifierField); ok {
		return f.ID
	}
	return 0
}

// olakeIcebergTypeToIceberg maps OLake's iceberg type strings (the
// values returned by types.DataType.ToIceberg()) into iceberg-go types.
func olakeIcebergTypeToIceberg(t string) (iceberg.Type, error) {
	switch strings.ToLower(strings.TrimSpace(t)) {
	case "boolean":
		return iceberg.PrimitiveTypes.Bool, nil
	case "int":
		return iceberg.PrimitiveTypes.Int32, nil
	case "long":
		return iceberg.PrimitiveTypes.Int64, nil
	case "float":
		return iceberg.PrimitiveTypes.Float32, nil
	case "double":
		return iceberg.PrimitiveTypes.Float64, nil
	case "string":
		return iceberg.PrimitiveTypes.String, nil
	case "timestamptz":
		return iceberg.PrimitiveTypes.TimestampTz, nil
	case "date":
		return iceberg.PrimitiveTypes.Date, nil
	default:
		// Fallback to string for any type the legacy path stringified.
		return iceberg.PrimitiveTypes.String, nil
	}
}

// icebergSchemaToColumnTypeMap inverts olakeIcebergTypeToIceberg.
func icebergSchemaToColumnTypeMap(sc *iceberg.Schema) map[string]string {
	out := make(map[string]string, sc.NumFields())
	for _, f := range sc.Fields() {
		out[f.Name] = icebergTypeToOlake(f.Type)
	}
	return out
}

func icebergTypeToOlake(t iceberg.Type) string {
	switch t.(type) {
	case iceberg.BooleanType:
		return "boolean"
	case iceberg.Int32Type:
		return "int"
	case iceberg.Int64Type:
		return "long"
	case iceberg.Float32Type:
		return "float"
	case iceberg.Float64Type:
		return "double"
	case iceberg.StringType:
		return "string"
	case iceberg.TimestampTzType:
		return "timestamptz"
	case iceberg.TimestampType:
		return "timestamptz"
	case iceberg.DateType:
		return "date"
	default:
		return "string"
	}
}

// parseIcebergTransformString accepts the OLake transform syntax
// (identity, void, year, month, day, hour, bucket[N], truncate[N]) and
// returns the matching iceberg.Transform implementation.
func parseIcebergTransformString(transform string) (iceberg.Transform, error) {
	base, arg, err := parseTransform(transform)
	if err != nil {
		return nil, err
	}
	switch base {
	case "identity":
		return iceberg.IdentityTransform{}, nil
	case "void":
		return iceberg.VoidTransform{}, nil
	case "year":
		return iceberg.YearTransform{}, nil
	case "month":
		return iceberg.MonthTransform{}, nil
	case "day":
		return iceberg.DayTransform{}, nil
	case "hour":
		return iceberg.HourTransform{}, nil
	case "bucket":
		if arg <= 0 {
			return nil, fmt.Errorf("bucket transform requires a positive arg")
		}
		return iceberg.BucketTransform{NumBuckets: arg}, nil
	case "truncate":
		if arg <= 0 {
			return nil, fmt.Errorf("truncate transform requires a positive arg")
		}
		return iceberg.TruncateTransform{Width: arg}, nil
	default:
		return nil, fmt.Errorf("unknown transform %q", transform)
	}
}

func isAlreadyExistsErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "already exists") || strings.Contains(msg, "alreadyexists")
}

func snapshotIDFor(tbl *icetable.Table) string {
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return "<none>"
	}
	return fmt.Sprintf("%d", snap.SnapshotID)
}
