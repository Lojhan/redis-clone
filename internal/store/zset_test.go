package store

import (
	"testing"
)

func TestZSetAdd(t *testing.T) {
	zs := NewZSet()

	added := zs.Add(1.0, "one")
	if !added {
		t.Error("Expected Add to return true for new member")
	}

	if zs.Card() != 1 {
		t.Errorf("Expected cardinality 1, got %d", zs.Card())
	}

	added = zs.Add(2.0, "one")
	if added {
		t.Error("Expected Add to return false for existing member")
	}

	if zs.Card() != 1 {
		t.Errorf("Expected cardinality 1 after update, got %d", zs.Card())
	}

	score, exists := zs.Score("one")
	if !exists || score != 2.0 {
		t.Errorf("Expected score 2.0, got %f", score)
	}
}

func TestZSetRemove(t *testing.T) {
	zs := NewZSet()

	zs.Add(1.0, "one")
	zs.Add(2.0, "two")

	removed := zs.Remove("one")
	if !removed {
		t.Error("Expected Remove to return true")
	}

	if zs.Card() != 1 {
		t.Errorf("Expected cardinality 1, got %d", zs.Card())
	}

	removed = zs.Remove("three")
	if removed {
		t.Error("Expected Remove to return false for non-existent member")
	}
}

func TestZSetScore(t *testing.T) {
	zs := NewZSet()

	zs.Add(1.5, "one")
	zs.Add(2.5, "two")

	score, exists := zs.Score("one")
	if !exists || score != 1.5 {
		t.Errorf("Expected score 1.5, got %f (exists: %v)", score, exists)
	}

	score, exists = zs.Score("two")
	if !exists || score != 2.5 {
		t.Errorf("Expected score 2.5, got %f (exists: %v)", score, exists)
	}

	_, exists = zs.Score("three")
	if exists {
		t.Error("Expected Score to return false for non-existent member")
	}
}

func TestZSetCard(t *testing.T) {
	zs := NewZSet()

	if zs.Card() != 0 {
		t.Errorf("Expected cardinality 0 for empty zset, got %d", zs.Card())
	}

	zs.Add(1.0, "one")
	zs.Add(2.0, "two")
	zs.Add(3.0, "three")

	if zs.Card() != 3 {
		t.Errorf("Expected cardinality 3, got %d", zs.Card())
	}
}

func TestZSetRank(t *testing.T) {
	zs := NewZSet()

	zs.Add(1.0, "one")
	zs.Add(2.0, "two")
	zs.Add(3.0, "three")

	rank, exists := zs.Rank("one")
	if !exists || rank != 0 {
		t.Errorf("Expected rank 0 for 'one', got %d (exists: %v)", rank, exists)
	}

	rank, exists = zs.Rank("two")
	if !exists || rank != 1 {
		t.Errorf("Expected rank 1 for 'two', got %d (exists: %v)", rank, exists)
	}

	rank, exists = zs.Rank("three")
	if !exists || rank != 2 {
		t.Errorf("Expected rank 2 for 'three', got %d (exists: %v)", rank, exists)
	}

	_, exists = zs.Rank("four")
	if exists {
		t.Error("Expected Rank to return false for non-existent member")
	}
}

func TestZSetRange(t *testing.T) {
	zs := NewZSet()

	zs.Add(1.0, "one")
	zs.Add(2.0, "two")
	zs.Add(3.0, "three")
	zs.Add(4.0, "four")
	zs.Add(5.0, "five")

	tests := []struct {
		start, stop int64
		expected    []string
	}{
		{0, 2, []string{"one", "two", "three"}},
		{1, 3, []string{"two", "three", "four"}},
		{0, 4, []string{"one", "two", "three", "four", "five"}},
		{2, 2, []string{"three"}},
		{4, 4, []string{"five"}},
	}

	for _, tt := range tests {
		members := zs.Range(tt.start, tt.stop)
		if len(members) != len(tt.expected) {
			t.Errorf("Range(%d, %d): expected %d members, got %d",
				tt.start, tt.stop, len(tt.expected), len(members))
			continue
		}

		for i, zm := range members {
			if zm.Member != tt.expected[i] {
				t.Errorf("Range(%d, %d): expected member %d to be '%s', got '%s'",
					tt.start, tt.stop, i, tt.expected[i], zm.Member)
			}
		}
	}
}

func TestZSetRangeWithScores(t *testing.T) {
	zs := NewZSet()

	zs.Add(1.5, "one")
	zs.Add(2.5, "two")
	zs.Add(3.5, "three")

	members := zs.Range(0, 2)

	expectedScores := []float64{1.5, 2.5, 3.5}
	for i, zm := range members {
		if zm.Score != expectedScores[i] {
			t.Errorf("Expected score %f at index %d, got %f", expectedScores[i], i, zm.Score)
		}
	}
}

func TestZSetAddUpdateScore(t *testing.T) {
	zs := NewZSet()

	zs.Add(1.0, "a")
	zs.Add(2.0, "b")
	zs.Add(3.0, "c")

	zs.Add(4.0, "b")

	members := zs.Range(0, 2)
	expected := []string{"a", "c", "b"}

	for i, zm := range members {
		if zm.Member != expected[i] {
			t.Errorf("Expected member %d to be '%s' after update, got '%s'",
				i, expected[i], zm.Member)
		}
	}

	score, _ := zs.Score("b")
	if score != 4.0 {
		t.Errorf("Expected updated score 4.0, got %f", score)
	}
}

func TestZSetSameScoreDifferentMembers(t *testing.T) {
	zs := NewZSet()

	zs.Add(1.0, "c")
	zs.Add(1.0, "a")
	zs.Add(1.0, "b")

	members := zs.Range(0, 2)
	expected := []string{"a", "b", "c"}

	for i, zm := range members {
		if zm.Member != expected[i] {
			t.Errorf("Expected member %d to be '%s', got '%s'",
				i, expected[i], zm.Member)
		}
	}
}

func TestZSetMultipleOperations(t *testing.T) {
	zs := NewZSet()

	zs.Add(1.0, "one")
	zs.Add(2.0, "two")
	zs.Add(3.0, "three")

	zs.Add(0.5, "two")

	members := zs.Range(0, 2)
	if members[0].Member != "two" || members[0].Score != 0.5 {
		t.Errorf("Expected first member to be (two, 0.5), got (%s, %f)",
			members[0].Member, members[0].Score)
	}

	zs.Remove("one")

	if zs.Card() != 2 {
		t.Errorf("Expected cardinality 2, got %d", zs.Card())
	}

	zs.Add(4.0, "one")

	if zs.Card() != 3 {
		t.Errorf("Expected cardinality 3 after re-adding, got %d", zs.Card())
	}

	members = zs.Range(0, 2)
	if len(members) != 3 {
		t.Errorf("Expected 3 members in range, got %d", len(members))
	}
	if members[2].Member != "one" || members[2].Score != 4.0 {
		t.Errorf("Expected last member to be (one, 4.0), got (%s, %f)",
			members[2].Member, members[2].Score)
	}
}
