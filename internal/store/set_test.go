package store

import (
	"testing"
)

func TestSetAdd(t *testing.T) {
	set := NewSet()

	added := set.Add("member1", "member2", "member3")
	if added != 3 {
		t.Errorf("Expected 3 members added, got %d", added)
	}

	added = set.Add("member1")
	if added != 0 {
		t.Errorf("Expected 0 members added (already exists), got %d", added)
	}

	added = set.Add("member2", "member4", "member5")
	if added != 2 {
		t.Errorf("Expected 2 new members added, got %d", added)
	}

	if set.Card() != 5 {
		t.Errorf("Expected cardinality 5, got %d", set.Card())
	}
}

func TestSetRemove(t *testing.T) {
	set := NewSet()
	set.Add("member1", "member2", "member3")

	removed := set.Remove("member1")
	if removed != 1 {
		t.Errorf("Expected 1 member removed, got %d", removed)
	}

	removed = set.Remove("nonexistent")
	if removed != 0 {
		t.Errorf("Expected 0 members removed, got %d", removed)
	}

	removed = set.Remove("member2", "member3", "nonexistent")
	if removed != 2 {
		t.Errorf("Expected 2 members removed, got %d", removed)
	}

	if set.Card() != 0 {
		t.Errorf("Expected cardinality 0, got %d", set.Card())
	}
}

func TestSetIsMember(t *testing.T) {
	set := NewSet()
	set.Add("member1", "member2")

	if !set.IsMember("member1") {
		t.Error("Expected member1 to be in set")
	}

	if set.IsMember("nonexistent") {
		t.Error("Expected nonexistent to not be in set")
	}

	set.Remove("member1")
	if set.IsMember("member1") {
		t.Error("Expected member1 to not be in set after removal")
	}
}

func TestSetMembers(t *testing.T) {
	set := NewSet()

	members := set.Members()
	if len(members) != 0 {
		t.Errorf("Expected 0 members, got %d", len(members))
	}

	set.Add("a", "b", "c")
	members = set.Members()

	if len(members) != 3 {
		t.Errorf("Expected 3 members, got %d", len(members))
	}

	expected := map[string]bool{"a": true, "b": true, "c": true}
	for _, member := range members {
		if !expected[member] {
			t.Errorf("Unexpected member: %s", member)
		}
		delete(expected, member)
	}

	if len(expected) != 0 {
		t.Errorf("Missing members: %v", expected)
	}
}

func TestSetCard(t *testing.T) {
	set := NewSet()

	if set.Card() != 0 {
		t.Errorf("Expected cardinality 0, got %d", set.Card())
	}

	set.Add("a", "b", "c")
	if set.Card() != 3 {
		t.Errorf("Expected cardinality 3, got %d", set.Card())
	}

	set.Add("a")
	if set.Card() != 3 {
		t.Errorf("Expected cardinality 3 after adding duplicate, got %d", set.Card())
	}

	set.Remove("b")
	if set.Card() != 2 {
		t.Errorf("Expected cardinality 2 after removal, got %d", set.Card())
	}
}

func TestSetPop(t *testing.T) {
	set := NewSet()

	_, ok := set.Pop()
	if ok {
		t.Error("Expected pop from empty set to return false")
	}

	set.Add("a", "b", "c")
	initialCard := set.Card()

	member, ok := set.Pop()
	if !ok {
		t.Error("Expected successful pop")
	}

	expectedMembers := map[string]bool{"a": true, "b": true, "c": true}
	if !expectedMembers[member] {
		t.Errorf("Unexpected member popped: %s", member)
	}

	if set.Card() != initialCard-1 {
		t.Errorf("Expected cardinality %d after pop, got %d", initialCard-1, set.Card())
	}

	if set.IsMember(member) {
		t.Errorf("Expected %s to be removed from set", member)
	}
}

func TestSetMultipleOperations(t *testing.T) {
	set := NewSet()

	set.Add("a", "b", "c")
	set.Remove("b")
	set.Add("d", "e")
	set.Remove("c", "e")
	set.Add("b")

	if set.Card() != 3 {
		t.Errorf("Expected cardinality 3, got %d", set.Card())
	}

	expectedMembers := map[string]bool{"a": true, "b": true, "d": true}
	members := set.Members()

	for _, member := range members {
		if !expectedMembers[member] {
			t.Errorf("Unexpected member in final set: %s", member)
		}
		delete(expectedMembers, member)
	}

	if len(expectedMembers) != 0 {
		t.Errorf("Missing members in final set: %v", expectedMembers)
	}
}
