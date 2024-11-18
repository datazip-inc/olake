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

func (this *Set[T]) WithHasher(f func(T) string) *Set[T] {
	this.funcHash = f

	return this
}

func (this *Set[T]) Hash(elem T) string {
	hashable, yes := any(elem).(Hashable)
	if yes {
		return hashable.Hash()
	}

	identifiable, yes := any(elem).(Identifier)
	if yes {
		return identifiable.ID()
	}

	if this.funcHash != nil {
		return this.funcHash(elem)
	}

	uniqueHash, err := hashstructure.Hash(elem, nil)
	if err != nil {
		// TODO: Handle this
		return "false"
	}

	return fmt.Sprintf("%d", uniqueHash)
}

// Find the difference between two sets
func (this *Set[T]) Difference(set *Set[T]) *Set[T] {
	difference := NewSet[T]()

	for k := range this.hash {
		if _, exists := set.hash[k]; !exists {
			difference.Insert(this.storage[k])
		}
	}

	return difference
}

// Call f for each item in the set
func (this *Set[T]) Range(f func(T)) {
	for _, value := range this.storage {
		f(value)
	}
}

// Test to see whether or not the element is in the set
func (this *Set[T]) Exists(element T) bool {
	_, exists := this.hash[this.Hash(element)]
	return exists
}

// Add an element to the set
func (this *Set[T]) Insert(elements ...T) {
	for _, elem := range elements {
		if this.Exists(elem) {
			continue
		}

		hash := this.Hash(elem)

		this.hash[hash] = nothing{}
		this.storage[hash] = elem
	}
}

// Find the intersection of two sets
func (this *Set[T]) Intersection(set *Set[T]) *Set[T] {
	subset := NewSet[T]()

	for k := range this.hash {
		if _, exists := set.hash[k]; exists {
			subset.Insert(set.storage[k])
		}
	}

	return subset
}

// Return the number of items in the set
func (this *Set[T]) Len() int {
	return len(this.hash)
}

// Test whether or not this set is a proper subset of "set"
func (this *Set[T]) ProperSubsetOf(set *Set[T]) bool {
	return this.SubsetOf(set) && this.Len() < set.Len()
}

// Remove an element from the set
func (this *Set[T]) Remove(element T) {
	hash := this.Hash(element)

	delete(this.hash, hash)
	delete(this.storage, hash)
}

// Test whether or not this set is a subset of "set"
func (this *Set[T]) SubsetOf(set *Set[T]) bool {
	if this.Len() > set.Len() {
		return false
	}
	for k := range this.hash {
		if _, exists := set.hash[k]; !exists {
			return false
		}
	}
	return true
}

// Find the union of two sets
func (this *Set[T]) Union(set *Set[T]) *Set[T] {
	union := NewSet[T]()

	for k := range this.hash {
		union.Insert(this.storage[k])
	}
	for k := range set.hash {
		union.Insert(set.storage[k])
	}

	return union
}

func (this *Set[T]) String() string {
	values := []string{}

	for _, value := range this.storage {
		values = append(values, fmt.Sprint(value))
	}

	return fmt.Sprintf("[%s]", strings.Join(values, ", "))
}

func (this *Set[T]) Array() []T {
	arr := []T{}

	for _, value := range this.storage {
		arr = append(arr, value)
	}

	return arr
}

func (this *Set[T]) UnmarshalJSON(data []byte) error {
	arr := []T{}
	err := json.Unmarshal(data, &arr)
	if err != nil {
		return err
	}

	for _, item := range arr {
		this.Insert(item)
	}

	return nil
}

func (this *Set[T]) MarshalJSON() ([]byte, error) {
	return json.Marshal(this.Array())
}
