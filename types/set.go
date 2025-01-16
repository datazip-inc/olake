package types

import (
	"fmt"
	"strings"

	"github.com/goccy/go-json"

	"github.com/mitchellh/hashstructure"
)

type Hashable interface {
	Hash() string
}

type Identifier interface {
	ID() string
}

type (
	Set[T comparable] struct {
		hash     map[string]nothing
		storage  map[string]T
		funcHash func(T) string
	}

	nothing struct{}
)

// Create a new set
func NewSet[T comparable](initial ...T) *Set[T] {
	s := &Set[T]{
		hash:    make(map[string]nothing),
		storage: make(map[string]T),
	}

	for _, v := range initial {
		s.Insert(v)
	}

	return s
}

func (st *Set[T]) WithHasher(f func(T) string) *Set[T] {
	st.funcHash = f

	return st
}

func (st *Set[T]) Hash(elem T) string {
	hashable, yes := any(elem).(Hashable)
	if yes {
		return hashable.Hash()
	}

	identifiable, yes := any(elem).(Identifier)
	if yes {
		return identifiable.ID()
	}

	if st.funcHash != nil {
		return st.funcHash(elem)
	}

	uniqueHash, err := hashstructure.Hash(elem, nil)
	if err != nil {
		// TODO: Handle st
		return "false"
	}

	return fmt.Sprintf("%d", uniqueHash)
}

// Find the difference between two sets
func (st *Set[T]) Difference(set *Set[T]) *Set[T] {
	difference := NewSet[T]()

	for k := range st.hash {
		if _, exists := set.hash[k]; !exists {
			difference.Insert(st.storage[k])
		}
	}

	return difference
}

// Call f for each item in the set
func (st *Set[T]) Range(f func(T)) {
	for _, value := range st.storage {
		f(value)
	}
}

// Test to see whether or not the element is in the set
func (st *Set[T]) Exists(element T) bool {
	_, exists := st.hash[st.Hash(element)]
	return exists
}

// Add an element to the set
func (st *Set[T]) Insert(elements ...T) {
	for _, elem := range elements {
		if st.Exists(elem) {
			continue
		}

		hash := st.Hash(elem)

		st.hash[hash] = nothing{}
		st.storage[hash] = elem
	}
}

// Find the intersection of two sets
func (st *Set[T]) Intersection(set *Set[T]) *Set[T] {
	subset := NewSet[T]()

	for k := range st.hash {
		if _, exists := set.hash[k]; exists {
			subset.Insert(set.storage[k])
		}
	}

	return subset
}

// Return the number of items in the set
func (st *Set[T]) Len() int {
	return len(st.hash)
}

// Test whether or not st set is a proper subset of "set"
func (st *Set[T]) ProperSubsetOf(set *Set[T]) bool {
	return st.SubsetOf(set) && st.Len() < set.Len()
}

// Remove an element from the set
func (st *Set[T]) Remove(element T) {
	hash := st.Hash(element)

	delete(st.hash, hash)
	delete(st.storage, hash)
}

// Test whether or not st set is a subset of "set"
func (st *Set[T]) SubsetOf(set *Set[T]) bool {
	if st.Len() > set.Len() {
		return false
	}
	for k := range st.hash {
		if _, exists := set.hash[k]; !exists {
			return false
		}
	}
	return true
}

// Find the union of two sets
func (st *Set[T]) Union(set *Set[T]) *Set[T] {
	union := NewSet[T]()

	for k := range st.hash {
		union.Insert(st.storage[k])
	}
	for k := range set.hash {
		union.Insert(set.storage[k])
	}

	return union
}

func (st *Set[T]) String() string {
	values := []string{}

	for _, value := range st.storage {
		values = append(values, fmt.Sprint(value))
	}

	return fmt.Sprintf("[%s]", strings.Join(values, ", "))
}

func (st *Set[T]) Array() []T {
	arr := []T{}

	for _, value := range st.storage {
		arr = append(arr, value)
	}

	return arr
}

func (st *Set[T]) UnmarshalJSON(data []byte) error {
	// to init underlying field during unmarshalling
	*st = *NewSet[T]()
	arr := []T{}
	err := json.Unmarshal(data, &arr)
	if err != nil {
		return err
	}

	for _, item := range arr {
		st.Insert(item)
	}

	return nil
}

func (st *Set[T]) MarshalJSON() ([]byte, error) {
	return json.Marshal(st.Array())
}
