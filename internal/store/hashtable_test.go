package store

import (
	"testing"
)

func TestHashTableSetGet(t *testing.T) {
	ht := NewHashTable()

	isNew := ht.Set("field1", "value1")
	if !isNew {
		t.Error("Expected isNew to be true for new field")
	}

	value, exists := ht.Get("field1")
	if !exists {
		t.Error("Expected field1 to exist")
	}
	if value != "value1" {
		t.Errorf("Expected value1, got %s", value)
	}

	isNew = ht.Set("field1", "value2")
	if isNew {
		t.Error("Expected isNew to be false for updated field")
	}

	value, exists = ht.Get("field1")
	if !exists {
		t.Error("Expected field1 to exist after update")
	}
	if value != "value2" {
		t.Errorf("Expected value2, got %s", value)
	}

	_, exists = ht.Get("nonexistent")
	if exists {
		t.Error("Expected nonexistent field to not exist")
	}
}

func TestHashTableDelete(t *testing.T) {
	ht := NewHashTable()

	ht.Set("field1", "value1")
	ht.Set("field2", "value2")

	deleted := ht.Delete("field1")
	if !deleted {
		t.Error("Expected field1 to be deleted")
	}

	_, exists := ht.Get("field1")
	if exists {
		t.Error("Expected field1 to not exist after deletion")
	}

	deleted = ht.Delete("nonexistent")
	if deleted {
		t.Error("Expected delete of nonexistent field to return false")
	}

	_, exists = ht.Get("field2")
	if !exists {
		t.Error("Expected field2 to still exist")
	}
}

func TestHashTableExists(t *testing.T) {
	ht := NewHashTable()

	if ht.Exists("field1") {
		t.Error("Expected field1 to not exist")
	}

	ht.Set("field1", "value1")

	if !ht.Exists("field1") {
		t.Error("Expected field1 to exist")
	}
}

func TestHashTableLen(t *testing.T) {
	ht := NewHashTable()

	if ht.Len() != 0 {
		t.Errorf("Expected length 0, got %d", ht.Len())
	}

	ht.Set("field1", "value1")
	ht.Set("field2", "value2")
	ht.Set("field3", "value3")

	if ht.Len() != 3 {
		t.Errorf("Expected length 3, got %d", ht.Len())
	}

	ht.Set("field1", "newvalue")
	if ht.Len() != 3 {
		t.Errorf("Expected length 3 after update, got %d", ht.Len())
	}

	ht.Delete("field2")
	if ht.Len() != 2 {
		t.Errorf("Expected length 2 after delete, got %d", ht.Len())
	}
}

func TestHashTableGetAll(t *testing.T) {
	ht := NewHashTable()

	all := ht.GetAll()
	if len(all) != 0 {
		t.Errorf("Expected empty map, got %d entries", len(all))
	}

	ht.Set("field1", "value1")
	ht.Set("field2", "value2")
	ht.Set("field3", "value3")

	all = ht.GetAll()
	if len(all) != 3 {
		t.Errorf("Expected 3 entries, got %d", len(all))
	}

	expected := map[string]string{
		"field1": "value1",
		"field2": "value2",
		"field3": "value3",
	}

	for field, expectedValue := range expected {
		value, exists := all[field]
		if !exists {
			t.Errorf("Expected field %s to exist in GetAll result", field)
		}
		if value != expectedValue {
			t.Errorf("Expected %s for field %s, got %s", expectedValue, field, value)
		}
	}
}

func TestHashTableFields(t *testing.T) {
	ht := NewHashTable()

	ht.Set("field1", "value1")
	ht.Set("field2", "value2")
	ht.Set("field3", "value3")

	fields := ht.Fields()
	if len(fields) != 3 {
		t.Errorf("Expected 3 fields, got %d", len(fields))
	}

	expectedFields := map[string]bool{
		"field1": true,
		"field2": true,
		"field3": true,
	}

	for _, field := range fields {
		if !expectedFields[field] {
			t.Errorf("Unexpected field: %s", field)
		}
		delete(expectedFields, field)
	}

	if len(expectedFields) != 0 {
		t.Errorf("Missing fields: %v", expectedFields)
	}
}

func TestHashTableValues(t *testing.T) {
	ht := NewHashTable()

	ht.Set("field1", "value1")
	ht.Set("field2", "value2")
	ht.Set("field3", "value3")

	values := ht.Values()
	if len(values) != 3 {
		t.Errorf("Expected 3 values, got %d", len(values))
	}

	expectedValues := map[string]bool{
		"value1": true,
		"value2": true,
		"value3": true,
	}

	for _, value := range values {
		if !expectedValues[value] {
			t.Errorf("Unexpected value: %s", value)
		}
		delete(expectedValues, value)
	}

	if len(expectedValues) != 0 {
		t.Errorf("Missing values: %v", expectedValues)
	}
}

func TestHashTableMultipleOperations(t *testing.T) {
	ht := NewHashTable()

	ht.Set("a", "1")
	ht.Set("b", "2")
	ht.Set("c", "3")
	ht.Delete("b")
	ht.Set("d", "4")
	ht.Set("a", "5")

	if ht.Len() != 3 {
		t.Errorf("Expected length 3, got %d", ht.Len())
	}

	if val, _ := ht.Get("a"); val != "5" {
		t.Errorf("Expected a=5, got a=%s", val)
	}

	if ht.Exists("b") {
		t.Error("Expected b to not exist")
	}

	if val, _ := ht.Get("c"); val != "3" {
		t.Errorf("Expected c=3, got c=%s", val)
	}

	if val, _ := ht.Get("d"); val != "4" {
		t.Errorf("Expected d=4, got d=%s", val)
	}
}
