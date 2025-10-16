package types

type PartitionMetaData struct {
	ReaderID    string
	Stream      StreamInterface
	PartitionID int
	EndOffset   int64
	StartOffset int64
}
