package types

import (
	"strconv"
	"testing"
)

func TestSetInsertHashesOnce(t *testing.T) {
	hashCalls := 0

	set := NewSet[int]().WithHasher(func(v int) string {
		hashCalls++
		return strconv.Itoa(v)
	})

	set.Insert(10)

	if hashCalls != 1 {
		t.Fatalf("expected 1 hash call, got %d", hashCalls)
	}
}

func TestSetInsertDuplicate(t *testing.T) {
	set := NewSet[int]()

	set.Insert(10)
	set.Insert(10)

	if set.Len() != 1 {
		t.Fatalf("expected set length 1, got %d", set.Len())
	}

	if !set.Exists(10) {
		t.Fatal("expected set to contain 10")
	}
}

type benchmarkItem struct {
	ID       int
	Name     string
	Database string
	Table    string
}

func BenchmarkSetInsert(b *testing.B) {
	for i := 0; i < b.N; i++ {
		set := NewSet[benchmarkItem]()

		for j := 0; j < 1000; j++ {
			set.Insert(benchmarkItem{
				ID:       j,
				Name:     "customer",
				Database: "production",
				Table:    "orders",
			})
		}
	}
}
