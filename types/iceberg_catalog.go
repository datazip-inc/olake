package types

import (
	"fmt"
	"slices"
	"strings"

	"github.com/datazip-inc/olake/utils"
)

// defaultCatalogType matches the iceberg destination, which treats a missing catalog_type as glue.
const defaultCatalogType = "glue"

type CatalogSpec struct {
	Catalog string `json:"catalog"`
	// Supports are the delete formats OLake can apply through this catalog.
	Supports []UpdateType `json:"supports"`
}

// icebergCatalogs is the write-capability matrix, one row per catalog_type the iceberg
// destination accepts. Most catalogs only store a pointer to the table's metadata, so they
// restrict delete formats only by refusing format-version 3 (needed for dv), refusing
// identifier fields (needed for eq), or rejecting delete files outright. Verified October 2026.
var icebergCatalogs = []CatalogSpec{
	{Catalog: "glue", Supports: []UpdateType{UpdateTypeEquality, UpdateTypePosition, UpdateTypeDeletionVector}},
	{Catalog: "jdbc", Supports: []UpdateType{UpdateTypeEquality, UpdateTypePosition, UpdateTypeDeletionVector}},
	{Catalog: "hive", Supports: []UpdateType{UpdateTypeEquality, UpdateTypePosition, UpdateTypeDeletionVector}},
	{Catalog: "rest", Supports: []UpdateType{UpdateTypeEquality, UpdateTypePosition, UpdateTypeDeletionVector}},
	{Catalog: "lakekeeper", Supports: []UpdateType{UpdateTypeEquality, UpdateTypePosition, UpdateTypeDeletionVector}},
	// Nessie does not support Iceberg table spec v3, so no deletion vectors.
	{Catalog: "nessie", Supports: []UpdateType{UpdateTypeEquality, UpdateTypePosition}},
	{Catalog: "s3tables", Supports: []UpdateType{UpdateTypeEquality, UpdateTypePosition, UpdateTypeDeletionVector}},
	// Unity rejects identifier columns, which eq keys on (without them OLake falls back to
	// append), and Databricks supports neither eq nor pos delete files, only deletion vectors.
	{Catalog: "unity", Supports: []UpdateType{UpdateTypeDeletionVector}},
	{Catalog: "polaris", Supports: []UpdateType{UpdateTypeEquality, UpdateTypePosition, UpdateTypeDeletionVector}},
	// Deletion vectors are in Preview on the BigLake (Lakehouse runtime) catalog.
	{Catalog: "biglake", Supports: []UpdateType{UpdateTypeEquality, UpdateTypePosition, UpdateTypeDeletionVector}},
	// Snowflake rejects equality deletes from external engines. dv works only on tables created
	// at v3: Horizon forbids upgrading an existing v2 table, which switching a stream to dv needs.
	{Catalog: "horizon", Supports: []UpdateType{UpdateTypePosition, UpdateTypeDeletionVector}},
}

// CatalogTypeFromConfig returns the destination's catalog_type, or "" for a destination that
// writes no delete files. Read raw because iceberg's config validation folds REST-family
// catalogs into "rest". Unknown catalogs are rejected rather than left unconstrained.
func CatalogTypeFromConfig(config *WriterConfig) (string, error) {
	if config == nil || config.Type != Iceberg {
		return "", nil
	}

	var writer struct {
		CatalogType string `json:"catalog_type"`
	}
	if err := utils.Unmarshal(config.WriterConfig, &writer); err != nil {
		return "", fmt.Errorf("failed to read destination writer config: %w", err)
	}

	catalog := writer.CatalogType
	if catalog == "" {
		catalog = defaultCatalogType
	}
	if _, found := lookupCatalog(catalog); !found {
		return "", fmt.Errorf("unknown catalog_type %q; supported are %s", catalog, strings.Join(catalogNames(), ", "))
	}

	return catalog, nil
}

// supportedByCatalog reports whether OLake can apply updateType through catalog. An empty
// catalog means no destination was given, which leaves the choice unconstrained.
func supportedByCatalog(catalog string, updateType UpdateType) bool {
	if catalog == "" {
		return true
	}
	spec, found := lookupCatalog(catalog)

	return found && slices.Contains(spec.Supports, updateType)
}

func lookupCatalog(catalog string) (CatalogSpec, bool) {
	idx := slices.IndexFunc(icebergCatalogs, func(spec CatalogSpec) bool { return spec.Catalog == catalog })
	if idx == -1 {
		return CatalogSpec{}, false
	}

	return icebergCatalogs[idx], true
}

func catalogNames() []string {
	names := make([]string, 0, len(icebergCatalogs))
	for _, spec := range icebergCatalogs {
		names = append(names, spec.Catalog)
	}

	return names
}
