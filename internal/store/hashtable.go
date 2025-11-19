package store

type HashTable struct {
	data map[string]string
}

func NewHashTable() *HashTable {
	return &HashTable{
		data: make(map[string]string),
	}
}

func (h *HashTable) Set(field, value string) bool {
	_, exists := h.data[field]
	h.data[field] = value
	return !exists
}

func (h *HashTable) Get(field string) (string, bool) {
	value, exists := h.data[field]
	return value, exists
}

func (h *HashTable) Delete(field string) bool {
	_, exists := h.data[field]
	if exists {
		delete(h.data, field)
	}
	return exists
}

func (h *HashTable) Exists(field string) bool {
	_, exists := h.data[field]
	return exists
}

func (h *HashTable) Len() int {
	return len(h.data)
}

func (h *HashTable) GetAll() map[string]string {
	result := make(map[string]string, len(h.data))
	for k, v := range h.data {
		result[k] = v
	}
	return result
}

func (h *HashTable) Fields() []string {
	fields := make([]string, 0, len(h.data))
	for field := range h.data {
		fields = append(fields, field)
	}
	return fields
}

func (h *HashTable) Values() []string {
	values := make([]string, 0, len(h.data))
	for _, value := range h.data {
		values = append(values, value)
	}
	return values
}
