package types

import "github.com/parquet-go/parquet-go"

type pqNodeConstructor func() parquet.Node

// destinationType holds every destination-side mapping for one olake DataType.
type destinationType struct {
	icebergType string
	parquetNode pqNodeConstructor
}

func leafNode(typ parquet.Type) pqNodeConstructor {
	return func() parquet.Node { return parquet.Leaf(typ) }
}

func timestampNode() parquet.Node { return parquet.Timestamp(parquet.Microsecond) }
