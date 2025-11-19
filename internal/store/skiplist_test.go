package store

import (
	"testing"
)

func TestSkiplistInsertAndLength(t *testing.T) {
	sl := newSkiplist()

	sl.insert(1.0, "one")
	sl.insert(2.0, "two")
	sl.insert(3.0, "three")

	if sl.length != 3 {
		t.Errorf("Expected length 3, got %d", sl.length)
	}
}

func TestSkiplistInsertOrder(t *testing.T) {
	sl := newSkiplist()

	sl.insert(3.0, "three")
	sl.insert(1.0, "one")
	sl.insert(2.0, "two")

	x := sl.first()
	if x == nil || x.member != "one" || x.score != 1.0 {
		t.Errorf("Expected first node to be (1.0, one), got (%f, %s)", x.score, x.member)
	}

	x = x.level[0].forward
	if x == nil || x.member != "two" || x.score != 2.0 {
		t.Errorf("Expected second node to be (2.0, two), got (%f, %s)", x.score, x.member)
	}

	x = x.level[0].forward
	if x == nil || x.member != "three" || x.score != 3.0 {
		t.Errorf("Expected third node to be (3.0, three), got (%f, %s)", x.score, x.member)
	}
}

func TestSkiplistInsertSameScore(t *testing.T) {
	sl := newSkiplist()

	sl.insert(1.0, "a")
	sl.insert(1.0, "c")
	sl.insert(1.0, "b")

	x := sl.first()
	if x.member != "a" {
		t.Errorf("Expected first member 'a', got '%s'", x.member)
	}

	x = x.level[0].forward
	if x.member != "b" {
		t.Errorf("Expected second member 'b', got '%s'", x.member)
	}

	x = x.level[0].forward
	if x.member != "c" {
		t.Errorf("Expected third member 'c', got '%s'", x.member)
	}
}

func TestSkiplistDelete(t *testing.T) {
	sl := newSkiplist()

	sl.insert(1.0, "one")
	sl.insert(2.0, "two")
	sl.insert(3.0, "three")

	deleted := sl.delete(2.0, "two")
	if !deleted {
		t.Error("Expected delete to return true")
	}

	if sl.length != 2 {
		t.Errorf("Expected length 2 after delete, got %d", sl.length)
	}

	x := sl.first()
	if x.member != "one" {
		t.Errorf("Expected first member 'one', got '%s'", x.member)
	}

	x = x.level[0].forward
	if x.member != "three" {
		t.Errorf("Expected second member 'three', got '%s'", x.member)
	}
}

func TestSkiplistDeleteNonExistent(t *testing.T) {
	sl := newSkiplist()

	sl.insert(1.0, "one")
	deleted := sl.delete(2.0, "two")

	if deleted {
		t.Error("Expected delete to return false for non-existent member")
	}

	if sl.length != 1 {
		t.Errorf("Expected length 1, got %d", sl.length)
	}
}

func TestSkiplistGetRank(t *testing.T) {
	sl := newSkiplist()

	sl.insert(1.0, "one")
	sl.insert(2.0, "two")
	sl.insert(3.0, "three")

	rank := sl.getRank(1.0, "one")
	if rank != 1 {
		t.Errorf("Expected rank 1, got %d", rank)
	}

	rank = sl.getRank(2.0, "two")
	if rank != 2 {
		t.Errorf("Expected rank 2, got %d", rank)
	}

	rank = sl.getRank(3.0, "three")
	if rank != 3 {
		t.Errorf("Expected rank 3, got %d", rank)
	}

	rank = sl.getRank(4.0, "four")
	if rank != 0 {
		t.Errorf("Expected rank 0 for non-existent member, got %d", rank)
	}
}

func TestSkiplistGetByRank(t *testing.T) {
	sl := newSkiplist()

	sl.insert(1.0, "one")
	sl.insert(2.0, "two")
	sl.insert(3.0, "three")

	node := sl.getByRank(1)
	if node == nil || node.member != "one" {
		t.Errorf("Expected member 'one' at rank 1, got '%s'", node.member)
	}

	node = sl.getByRank(2)
	if node == nil || node.member != "two" {
		t.Errorf("Expected member 'two' at rank 2, got '%s'", node.member)
	}

	node = sl.getByRank(3)
	if node == nil || node.member != "three" {
		t.Errorf("Expected member 'three' at rank 3, got '%s'", node.member)
	}

	node = sl.getByRank(0)
	if node != nil {
		t.Error("Expected nil for rank 0")
	}

	node = sl.getByRank(4)
	if node != nil {
		t.Error("Expected nil for rank > length")
	}
}

func TestSkiplistGetRange(t *testing.T) {
	sl := newSkiplist()

	sl.insert(1.0, "one")
	sl.insert(2.0, "two")
	sl.insert(3.0, "three")
	sl.insert(4.0, "four")
	sl.insert(5.0, "five")

	tests := []struct {
		start, stop int64
		expected    []string
	}{
		{1, 3, []string{"one", "two", "three"}},
		{2, 4, []string{"two", "three", "four"}},
		{1, 5, []string{"one", "two", "three", "four", "five"}},
		{3, 3, []string{"three"}},
		{5, 5, []string{"five"}},
	}

	for _, tt := range tests {
		nodes := sl.getRange(tt.start, tt.stop)
		if len(nodes) != len(tt.expected) {
			t.Errorf("getRange(%d, %d): expected %d nodes, got %d",
				tt.start, tt.stop, len(tt.expected), len(nodes))
			continue
		}

		for i, node := range nodes {
			if node.member != tt.expected[i] {
				t.Errorf("getRange(%d, %d): expected node %d to be '%s', got '%s'",
					tt.start, tt.stop, i, tt.expected[i], node.member)
			}
		}
	}
}

func TestSkiplistGetRangeInvalid(t *testing.T) {
	sl := newSkiplist()

	sl.insert(1.0, "one")
	sl.insert(2.0, "two")

	nodes := sl.getRange(0, 1)
	if nodes != nil {
		t.Error("Expected nil for start <= 0")
	}

	nodes = sl.getRange(3, 5)
	if nodes != nil {
		t.Error("Expected nil for start > length")
	}

	nodes = sl.getRange(2, 1)
	if nodes != nil {
		t.Error("Expected nil for start > stop")
	}
}

func TestSkiplistLargeDataset(t *testing.T) {
	sl := newSkiplist()

	for i := 0; i < 1000; i++ {
		sl.insert(float64(i), string(rune('a'+i%26)))
	}

	if sl.length != 1000 {
		t.Errorf("Expected length 1000, got %d", sl.length)
	}

	for i := int64(1); i <= 1000; i++ {
		node := sl.getByRank(i)
		if node == nil {
			t.Errorf("Expected node at rank %d", i)
			break
		}
	}
}
