package types

import (
	"sync/atomic"

	"github.com/datazip-inc/olake/utils"
)

type BatchSizeEstimator struct {
	avgrecordsize *atomic.Int64
	records       int64
	maxtoconsume  int64
}

func NewBatchSizeEstimator(input int64) *BatchSizeEstimator {
	return &BatchSizeEstimator{
		maxtoconsume: input,
	}
}

func (b *BatchSizeEstimator) Size() int64 {
	if b.avgrecordsize == nil {
		return b.maxtoconsume
	}

	return int64((float64(utils.FreeMemory()) * 0.8) / float64(b.avgrecordsize.Load()))
}

func (b *BatchSizeEstimator) Consume(data Record) {
	size := utils.SizeOf(data)
	if b.avgrecordsize == nil {
		b.avgrecordsize = &atomic.Int64{}
		b.avgrecordsize.Store(int64(size))
	} else if b.records <= b.maxtoconsume {
		b.avgrecordsize.Store((b.records*b.avgrecordsize.Load() + int64(size)) / (b.records + 1))
	}

	b.records += 1
}
