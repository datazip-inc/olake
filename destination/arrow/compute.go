package arrowdst

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
)

// ---------------------------------------------------------------------------
// Type-promotion rules — single source of truth for the arrow pipeline.
// ---------------------------------------------------------------------------

var validTransitions = map[types.DataType]map[types.DataType]bool{
	types.Int32:          {types.Int32: true, types.Int64: true},
	types.Int64:          {types.Int64: true, types.Int32: true},
	types.Float32:        {types.Float32: true, types.Float64: true},
	types.Float64:        {types.Float64: true, types.Float32: true},
	types.Bool:           {types.Bool: true},
	types.String:         {types.String: true},
	types.Timestamp:      {types.Timestamp: true, types.TimestampMilli: true, types.TimestampMicro: true, types.TimestampNano: true},
	types.TimestampMilli: {types.TimestampMilli: true, types.TimestampMicro: true, types.TimestampNano: true},
	types.TimestampMicro: {types.TimestampMicro: true, types.TimestampNano: true},
	types.TimestampNano:  {types.TimestampNano: true},
}

var promotionRequired = map[types.DataType]map[types.DataType]bool{
	types.Int32:          {types.Int64: true},
	types.Float32:        {types.Float64: true},
	types.Timestamp:      {types.TimestampMilli: true, types.TimestampMicro: true, types.TimestampNano: true},
	types.TimestampMilli: {types.TimestampMicro: true, types.TimestampNano: true},
	types.TimestampMicro: {types.TimestampNano: true},
}

func IsValidTransition(oldT, newT types.DataType) bool {
	if m, ok := validTransitions[oldT]; ok {
		return m[newT]
	}
	return oldT == newT
}

func IsPromotionRequired(oldT, newT types.DataType) bool {
	if m, ok := promotionRequired[oldT]; ok {
		return m[newT]
	}
	return false
}

// CloneSchema returns a shallow copy safe to mutate inside a single thread.
func CloneSchema(s OLakeSchema) OLakeSchema {
	out := make(OLakeSchema, len(s))
	for k, v := range s {
		out[k] = v
	}
	return out
}

// MergeSchemas merges a per-batch schema into the stream's current schema.
// New fields are added; existing fields are kept or promoted on valid
// transitions. changed is true when anything changed.
func MergeSchemas(base, batch OLakeSchema) (OLakeSchema, bool) {
	if len(batch) == 0 {
		return base, false
	}
	out := CloneSchema(base)
	changed := false
	for k, newT := range batch {
		oldT, exists := out[k]
		if !exists {
			out[k] = newT
			changed = true
			continue
		}
		if oldT == newT {
			continue
		}
		if IsPromotionRequired(oldT, newT) {
			out[k] = newT
			changed = true
			continue
		}
		// valid transition where old type wins — no-op
		// invalid transitions: keep base type; adapter handles downstream errors
	}
	return out, changed
}

// ---------------------------------------------------------------------------
// FlattenAndDetect — single destination-neutral compute entry point.
// ---------------------------------------------------------------------------

func FlattenAndDetect(ctx context.Context, stream types.StreamInterface, applyFilter bool,
	threadSchema OLakeSchema, preShape []PartitionPreShape, records []types.RawRecord,
) (bool, OLakeSchema, []types.RawRecord, error) {
	if !stream.NormalizationEnabled() {
		return flattenNonNormalized(records, preShape)
	}
	return flattenNormalized(ctx, stream, applyFilter, threadSchema, records)
}

func flattenNormalized(ctx context.Context, stream types.StreamInterface, applyFilter bool,
	threadSchema OLakeSchema, records []types.RawRecord,
) (bool, OLakeSchema, []types.RawRecord, error) {
	flattener := typeutils.NewFlattener()
	batchSchema := make(OLakeSchema)
	diff := false
	var mu sync.Mutex

	err := utils.Concurrent(ctx, records, len(records), func(_ context.Context, rec types.RawRecord, idx int) error {
		flat, ferr := flattener.Flatten(rec.Data)
		if ferr != nil {
			return fmt.Errorf("flatten: %s", ferr)
		}
		for k, v := range rec.OlakeColumns {
			flat[k] = v
		}
		local := make(map[string]types.DataType)
		for k, v := range flat {
			dt := typeutils.TypeFromValue(v)
			if dt == types.Null {
				delete(flat, k)
				continue
			}
			local[k] = dt
		}
		records[idx].Data = flat

		mu.Lock()
		for k, newT := range local {
			if existing, ok := batchSchema[k]; ok {
				batchSchema[k] = types.GetCommonAncestorType(existing, newT)
			} else {
				batchSchema[k] = newT
			}
			if base, ok := threadSchema[k]; !ok || base != batchSchema[k] {
				diff = true
			}
		}
		mu.Unlock()
		return nil
	})
	if err != nil {
		return false, nil, nil, fmt.Errorf("flatten records: %s", err)
	}

	kept := records
	if applyFilter {
		filter, isLegacy, ferr := stream.GetFilter()
		if ferr != nil {
			return false, nil, nil, fmt.Errorf("get filter: %s", ferr)
		}
		if isLegacy {
			logger.Warnf("Stream[%s]: legacy SQL filter is not supported by the arrow pipeline; skipping", stream.ID())
		} else {
			kept, err = typeutils.FilterRecords(ctx, records, filter, false, OLakeSchema(threadSchema))
			if err != nil {
				return false, nil, nil, fmt.Errorf("filter records: %s", err)
			}
		}
	}
	return diff, batchSchema, kept, nil
}

func flattenNonNormalized(records []types.RawRecord, preShape []PartitionPreShape) (bool, OLakeSchema, []types.RawRecord, error) {
	for i := range records {
		jsonBlob, err := json.Marshal(records[i].Data)
		if err != nil {
			return false, nil, nil, fmt.Errorf("marshal data column: %s", err)
		}
		newData := map[string]any{"data": string(jsonBlob)}
		for k, v := range records[i].OlakeColumns {
			newData[k] = v
		}
		for _, ps := range preShape {
			if ps.SourceField == constants.OlakeTimestamp {
				continue
			}
			if v, ok := records[i].Data[ps.SourceField]; ok {
				newData[ps.DestField] = v
			}
		}
		records[i].Data = newData
	}
	return false, nil, records, nil
}

// ---------------------------------------------------------------------------
// NormaliseOpType — op-type rewrite helper.
//
// NEVER called automatically by the core. Each adapter calls it at the
// correct point:
//   - Iceberg adapter: AFTER dedup.Track (dedup needs original "i")
//   - Parquet-arrow adapter: BEFORE BuildArrowRecord (unconditional)
// ---------------------------------------------------------------------------

func NormaliseOpType(rec *types.RawRecord) {
	if rec == nil || rec.OlakeColumns == nil {
		return
	}
	if op, ok := rec.OlakeColumns[constants.OpType].(string); ok && op == "i" {
		rec.OlakeColumns[constants.OpType] = "c"
	}
}
