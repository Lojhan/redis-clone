package store

import (
	"fmt"
	"testing"
)

func TestQuicklistPushHead(t *testing.T) {
	q := NewQuicklist()

	q.PushHead("three")
	q.PushHead("two")
	q.PushHead("one")

	if q.Len() != 3 {
		t.Errorf("Expected length 3, got %d", q.Len())
	}

	elements := q.ToSlice()
	expected := []string{"one", "two", "three"}

	if len(elements) != len(expected) {
		t.Fatalf("Expected %d elements, got %d", len(expected), len(elements))
	}

	for i, elem := range elements {
		if elem != expected[i] {
			t.Errorf("Element %d: expected %s, got %s", i, expected[i], elem)
		}
	}
}

func TestQuicklistPushTail(t *testing.T) {
	q := NewQuicklist()

	q.PushTail("one")
	q.PushTail("two")
	q.PushTail("three")

	if q.Len() != 3 {
		t.Errorf("Expected length 3, got %d", q.Len())
	}

	elements := q.ToSlice()
	expected := []string{"one", "two", "three"}

	for i, elem := range elements {
		if elem != expected[i] {
			t.Errorf("Element %d: expected %s, got %s", i, expected[i], elem)
		}
	}
}

func TestQuicklistPopHead(t *testing.T) {
	q := NewQuicklist()

	q.PushTail("one")
	q.PushTail("two")
	q.PushTail("three")

	val, ok := q.PopHead()
	if !ok {
		t.Fatal("PopHead failed")
	}
	if val != "one" {
		t.Errorf("Expected 'one', got '%s'", val)
	}

	val, ok = q.PopHead()
	if !ok {
		t.Fatal("PopHead failed")
	}
	if val != "two" {
		t.Errorf("Expected 'two', got '%s'", val)
	}

	if q.Len() != 1 {
		t.Errorf("Expected length 1, got %d", q.Len())
	}

	val, ok = q.PopHead()
	if !ok {
		t.Fatal("PopHead failed")
	}
	if val != "three" {
		t.Errorf("Expected 'three', got '%s'", val)
	}

	if q.Len() != 0 {
		t.Errorf("Expected length 0, got %d", q.Len())
	}

	_, ok = q.PopHead()
	if ok {
		t.Error("PopHead from empty list should return false")
	}
}

func TestQuicklistPopTail(t *testing.T) {
	q := NewQuicklist()

	q.PushTail("one")
	q.PushTail("two")
	q.PushTail("three")

	val, ok := q.PopTail()
	if !ok {
		t.Fatal("PopTail failed")
	}
	if val != "three" {
		t.Errorf("Expected 'three', got '%s'", val)
	}

	val, ok = q.PopTail()
	if !ok {
		t.Fatal("PopTail failed")
	}
	if val != "two" {
		t.Errorf("Expected 'two', got '%s'", val)
	}

	if q.Len() != 1 {
		t.Errorf("Expected length 1, got %d", q.Len())
	}

	val, ok = q.PopTail()
	if !ok {
		t.Fatal("PopTail failed")
	}
	if val != "one" {
		t.Errorf("Expected 'one', got '%s'", val)
	}

	if q.Len() != 0 {
		t.Errorf("Expected length 0, got %d", q.Len())
	}

	_, ok = q.PopTail()
	if ok {
		t.Error("PopTail from empty list should return false")
	}
}

func TestQuicklistRange(t *testing.T) {
	q := NewQuicklist()

	for i := range 10 {
		q.PushTail(fmt.Sprintf("elem%d", i))
	}

	tests := []struct {
		name     string
		start    int64
		stop     int64
		expected []string
	}{
		{
			name:     "full range",
			start:    0,
			stop:     9,
			expected: []string{"elem0", "elem1", "elem2", "elem3", "elem4", "elem5", "elem6", "elem7", "elem8", "elem9"},
		},
		{
			name:     "partial range",
			start:    2,
			stop:     5,
			expected: []string{"elem2", "elem3", "elem4", "elem5"},
		},
		{
			name:     "single element",
			start:    4,
			stop:     4,
			expected: []string{"elem4"},
		},
		{
			name:     "negative indices",
			start:    -3,
			stop:     -1,
			expected: []string{"elem7", "elem8", "elem9"},
		},
		{
			name:     "mixed indices",
			start:    -5,
			stop:     8,
			expected: []string{"elem5", "elem6", "elem7", "elem8"},
		},
		{
			name:     "out of bounds",
			start:    100,
			stop:     200,
			expected: []string{},
		},
		{
			name:     "invalid range",
			start:    5,
			stop:     2,
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := q.Range(tt.start, tt.stop)

			if len(result) != len(tt.expected) {
				t.Fatalf("Expected %d elements, got %d", len(tt.expected), len(result))
			}

			for i, elem := range result {
				if elem != tt.expected[i] {
					t.Errorf("Element %d: expected %s, got %s", i, tt.expected[i], elem)
				}
			}
		})
	}
}

func TestQuicklistIndex(t *testing.T) {
	q := NewQuicklist()

	for i := range 5 {
		q.PushTail(fmt.Sprintf("elem%d", i))
	}

	tests := []struct {
		name     string
		index    int64
		expected string
		ok       bool
	}{
		{
			name:     "first element",
			index:    0,
			expected: "elem0",
			ok:       true,
		},
		{
			name:     "middle element",
			index:    2,
			expected: "elem2",
			ok:       true,
		},
		{
			name:     "last element",
			index:    4,
			expected: "elem4",
			ok:       true,
		},
		{
			name:     "negative index -1",
			index:    -1,
			expected: "elem4",
			ok:       true,
		},
		{
			name:     "negative index -3",
			index:    -3,
			expected: "elem2",
			ok:       true,
		},
		{
			name:     "out of bounds positive",
			index:    10,
			expected: "",
			ok:       false,
		},
		{
			name:     "out of bounds negative",
			index:    -10,
			expected: "",
			ok:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, ok := q.Index(tt.index)

			if ok != tt.ok {
				t.Errorf("Expected ok=%v, got ok=%v", tt.ok, ok)
			}

			if ok && result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestQuicklistMultipleNodes(t *testing.T) {
	q := NewQuicklist()

	numElements := QuicklistNodeMaxEntries*2 + 50

	for i := range numElements {
		q.PushTail(fmt.Sprintf("elem%d", i))
	}

	if q.Len() != int64(numElements) {
		t.Errorf("Expected length %d, got %d", numElements, q.Len())
	}

	if q.count <= 1 {
		t.Errorf("Expected multiple nodes, got %d", q.count)
	}

	elements := q.ToSlice()
	if len(elements) != numElements {
		t.Errorf("Expected %d elements in slice, got %d", numElements, len(elements))
	}

	for i := range numElements {
		expected := fmt.Sprintf("elem%d", i)
		if elements[i] != expected {
			t.Errorf("Element %d: expected %s, got %s", i, expected, elements[i])
			break
		}
	}
}

func TestQuicklistMixedOperations(t *testing.T) {
	q := NewQuicklist()

	q.PushHead("2")
	q.PushHead("1")
	q.PushTail("3")
	q.PushTail("4")

	elements := q.ToSlice()
	expected := []string{"1", "2", "3", "4"}

	for i, elem := range elements {
		if elem != expected[i] {
			t.Errorf("Element %d: expected %s, got %s", i, expected[i], elem)
		}
	}

	val, _ := q.PopHead()
	if val != "1" {
		t.Errorf("Expected '1', got '%s'", val)
	}

	val, _ = q.PopTail()
	if val != "4" {
		t.Errorf("Expected '4', got '%s'", val)
	}

	if q.Len() != 2 {
		t.Errorf("Expected length 2, got %d", q.Len())
	}

	elements = q.ToSlice()
	expected = []string{"2", "3"}

	for i, elem := range elements {
		if elem != expected[i] {
			t.Errorf("Element %d: expected %s, got %s", i, expected[i], elem)
		}
	}
}

func TestQuicklistEmptyOperations(t *testing.T) {
	q := NewQuicklist()

	if q.Len() != 0 {
		t.Errorf("Expected length 0, got %d", q.Len())
	}

	_, ok := q.PopHead()
	if ok {
		t.Error("PopHead on empty list should return false")
	}

	_, ok = q.PopTail()
	if ok {
		t.Error("PopTail on empty list should return false")
	}

	elements := q.Range(0, 10)
	if len(elements) != 0 {
		t.Errorf("Range on empty list should return empty slice, got %d elements", len(elements))
	}

	_, ok = q.Index(0)
	if ok {
		t.Error("Index on empty list should return false")
	}
}
