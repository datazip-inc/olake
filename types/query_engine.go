package types

import (
	"fmt"
	"slices"
	"strings"
)

// QueryEngine is a downstream engine that reads the Iceberg tables OLake writes.
type QueryEngine string

const (
	QueryEngineSpark      QueryEngine = "spark"
	QueryEngineFlink      QueryEngine = "flink"
	QueryEngineTrino      QueryEngine = "trino"
	QueryEnginePresto     QueryEngine = "presto"
	QueryEngineStarRocks  QueryEngine = "starrocks"
	QueryEngineHive       QueryEngine = "hive"
	QueryEngineDuckDB     QueryEngine = "duckdb"
	QueryEngineAthena     QueryEngine = "athena"
	QueryEngineSnowflake  QueryEngine = "snowflake"
	QueryEngineBigQuery   QueryEngine = "bigquery"
	QueryEngineDatabricks QueryEngine = "databricks"
	QueryEngineDremio     QueryEngine = "dremio"
	QueryEngineClickHouse QueryEngine = "clickhouse"
)

type QueryEngineSpec struct {
	Engine QueryEngine `json:"engine"`
	Label  string      `json:"label"`
	// Supports are the delete formats the engine resolves at read time, independent of
	// what OLake can write (writableUpdateTypes).
	Supports []UpdateType `json:"supports"`
}

// writableUpdateTypes are the delete formats OLake can produce, cheapest first: equality
// needs no index, positional needs a full identifier -> RowLocation index. Deletion
// vectors are absent until the dv writer lands.
var writableUpdateTypes = []UpdateType{UpdateTypeEquality, UpdateTypePosition}

// queryEngines is the read-capability matrix. Support is version dependent and moves fast;
// each row reflects the engine's current stable release. Positional deletes are the
// universal denominator, equality deletes are read by far fewer engines.
var queryEngines = []QueryEngineSpec{
	{Engine: QueryEngineSpark, Label: "Apache Spark", Supports: []UpdateType{UpdateTypeEquality, UpdateTypePosition, UpdateTypeDeletionVector}},
	{Engine: QueryEngineFlink, Label: "Apache Flink", Supports: []UpdateType{UpdateTypeEquality, UpdateTypePosition}},
	{Engine: QueryEngineTrino, Label: "Trino", Supports: []UpdateType{UpdateTypeEquality, UpdateTypePosition, UpdateTypeDeletionVector}},
	{Engine: QueryEnginePresto, Label: "Presto", Supports: []UpdateType{UpdateTypeEquality, UpdateTypePosition}},
	{Engine: QueryEngineStarRocks, Label: "StarRocks", Supports: []UpdateType{UpdateTypeEquality, UpdateTypePosition}},
	{Engine: QueryEngineHive, Label: "Apache Hive", Supports: []UpdateType{UpdateTypePosition}},
	{Engine: QueryEngineDuckDB, Label: "DuckDB", Supports: []UpdateType{UpdateTypePosition}},
	{Engine: QueryEngineAthena, Label: "AWS Athena", Supports: []UpdateType{UpdateTypePosition}},
	{Engine: QueryEngineSnowflake, Label: "Snowflake", Supports: []UpdateType{UpdateTypePosition}},
	{Engine: QueryEngineBigQuery, Label: "Google BigQuery", Supports: []UpdateType{UpdateTypePosition}},
	{Engine: QueryEngineDatabricks, Label: "Databricks", Supports: []UpdateType{UpdateTypePosition, UpdateTypeDeletionVector}},
	{Engine: QueryEngineDremio, Label: "Dremio", Supports: []UpdateType{UpdateTypePosition}},
	{Engine: QueryEngineClickHouse, Label: "ClickHouse", Supports: []UpdateType{UpdateTypePosition}},
}

// QueryEngineCatalog is served by `spec --available-query-engines`. The engine names it
// returns are exactly the values --target-query-engines accepts.
func QueryEngineCatalog() map[string]interface{} {
	return map[string]interface{}{
		"engines":               queryEngines,
		"writable_update_types": writableUpdateTypes,
	}
}

// ParseQueryEngines normalizes raw values and rejects unknown names: a typo that silently
// dropped an engine would widen AvailableUpdateTypes and let an unreadable format through.
func ParseQueryEngines(values []string) ([]QueryEngine, error) {
	engines := make([]QueryEngine, 0, len(values))
	for _, value := range values {
		engine := QueryEngine(strings.ToLower(strings.TrimSpace(value)))
		if engine == "" {
			continue
		}
		if _, found := lookupEngine(engine); !found {
			return nil, fmt.Errorf("unknown query engine %q; supported are %s", value, strings.Join(engineNames(), ", "))
		}
		if !slices.Contains(engines, engine) {
			engines = append(engines, engine)
		}
	}

	return engines, nil
}

// AvailableUpdateTypes returns the writable delete formats every engine can read, cheapest
// first. No engines means the choice is unconstrained.
func AvailableUpdateTypes(engines []QueryEngine) []UpdateType {
	available := make([]UpdateType, 0, len(writableUpdateTypes))
	for _, updateType := range writableUpdateTypes {
		if readableByAll(engines, updateType) {
			available = append(available, updateType)
		}
	}

	return available
}

// PreferredUpdateType returns the cheapest available format, empty when none satisfies
// every engine.
func PreferredUpdateType(available []UpdateType) UpdateType {
	if len(available) == 0 {
		return ""
	}

	return available[0]
}

func readableByAll(engines []QueryEngine, updateType UpdateType) bool {
	for _, engine := range engines {
		spec, found := lookupEngine(engine)
		if !found || !slices.Contains(spec.Supports, updateType) {
			return false
		}
	}

	return true
}

func lookupEngine(engine QueryEngine) (QueryEngineSpec, bool) {
	idx := slices.IndexFunc(queryEngines, func(spec QueryEngineSpec) bool { return spec.Engine == engine })
	if idx == -1 {
		return QueryEngineSpec{}, false
	}

	return queryEngines[idx], true
}

func engineNames() []string {
	names := make([]string, 0, len(queryEngines))
	for _, spec := range queryEngines {
		names = append(names, string(spec.Engine))
	}

	return names
}
