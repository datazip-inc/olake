package types

import (
	"fmt"
	"slices"

	"github.com/datazip-inc/olake/utils"
)

// defaultCatalogType matches the iceberg destination, which treats a missing catalog_type as glue.
const defaultCatalogType = "glue"

// catalogRestrictions lists only the catalogs that cannot apply every delete format; any other
// catalog supports all of them. Most catalogs only store a pointer to the table's metadata, so
// they restrict delete formats only by refusing format-version 3 (needed for dv), refusing
// identifier fields (needed for eq), or rejecting delete files outright. Verified October 2026.
var catalogRestrictions = map[string][]UpdateType{
	// Nessie does not support Iceberg table spec v3, so no deletion vectors.
	"nessie": {UpdateTypeEquality, UpdateTypePosition},
	// Unity rejects identifier columns, which eq keys on (without them OLake falls back to
	// append), and Databricks supports neither eq nor pos delete files, only deletion vectors.
	"unity": {UpdateTypeDeletionVector},
	// Snowflake rejects equality deletes from external engines. dv works only on tables created
	// at v3: Horizon forbids upgrading an existing v2 table, which switching a stream to dv needs.
	"horizon": {UpdateTypePosition, UpdateTypeDeletionVector},
}

// CatalogTypeFromConfig returns the destination's catalog_type, or "" for a destination that
// writes no delete files. Read raw because iceberg's config validation folds REST-family
// catalogs into "rest", which would hide a restricted catalog.
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

	if writer.CatalogType == "" {
		return defaultCatalogType, nil
	}
	return writer.CatalogType, nil
}

// supportedByCatalog reports whether OLake can apply updateType through catalog. A catalog
// without restrictions, or none at all (no destination given), supports every format.
func supportedByCatalog(catalog string, updateType UpdateType) bool {
	supported, restricted := catalogRestrictions[catalog]
	return !restricted || slices.Contains(supported, updateType)
}
