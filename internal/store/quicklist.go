package store

const (
	QuicklistNodeMaxSize = 8192

	QuicklistNodeMaxEntries = 512
)

type QuicklistNode struct {
	prev    *QuicklistNode
	next    *QuicklistNode
	entries []string
	size    int
}

type Quicklist struct {
	head  *QuicklistNode
	tail  *QuicklistNode
	len   int64
	count int
}

func NewQuicklist() *Quicklist {
	return &Quicklist{
		head:  nil,
		tail:  nil,
		len:   0,
		count: 0,
	}
}

func (q *Quicklist) Len() int64 {
	return q.len
}

func (q *Quicklist) PushHead(value string) {

	if q.head == nil {
		node := &QuicklistNode{
			entries: []string{value},
			size:    len(value),
		}
		q.head = node
		q.tail = node
		q.count = 1
		q.len = 1
		return
	}

	if q.canAddToNode(q.head, value) {

		q.head.entries = append([]string{value}, q.head.entries...)
		q.head.size += len(value)
		q.len++
		return
	}

	node := &QuicklistNode{
		next:    q.head,
		entries: []string{value},
		size:    len(value),
	}
	q.head.prev = node
	q.head = node
	q.count++
	q.len++
}

func (q *Quicklist) PushTail(value string) {

	if q.tail == nil {
		node := &QuicklistNode{
			entries: []string{value},
			size:    len(value),
		}
		q.head = node
		q.tail = node
		q.count = 1
		q.len = 1
		return
	}

	if q.canAddToNode(q.tail, value) {

		q.tail.entries = append(q.tail.entries, value)
		q.tail.size += len(value)
		q.len++
		return
	}

	node := &QuicklistNode{
		prev:    q.tail,
		entries: []string{value},
		size:    len(value),
	}
	q.tail.next = node
	q.tail = node
	q.count++
	q.len++
}

func (q *Quicklist) PopHead() (string, bool) {
	if q.head == nil {
		return "", false
	}

	value := q.head.entries[0]
	q.head.entries = q.head.entries[1:]
	q.head.size -= len(value)
	q.len--

	if len(q.head.entries) == 0 {
		q.head = q.head.next
		if q.head != nil {
			q.head.prev = nil
		} else {
			q.tail = nil
		}
		q.count--
	}

	return value, true
}

func (q *Quicklist) PopTail() (string, bool) {
	if q.tail == nil {
		return "", false
	}

	lastIdx := len(q.tail.entries) - 1
	value := q.tail.entries[lastIdx]
	q.tail.entries = q.tail.entries[:lastIdx]
	q.tail.size -= len(value)
	q.len--

	if len(q.tail.entries) == 0 {
		q.tail = q.tail.prev
		if q.tail != nil {
			q.tail.next = nil
		} else {
			q.head = nil
		}
		q.count--
	}

	return value, true
}

func (q *Quicklist) Range(start, stop int64) []string {
	if q.len == 0 {
		return []string{}
	}

	if start < 0 {
		start = q.len + start
	}
	if stop < 0 {
		stop = q.len + stop
	}

	if start < 0 {
		start = 0
	}
	if stop >= q.len {
		stop = q.len - 1
	}
	if start > stop || start >= q.len {
		return []string{}
	}

	result := make([]string, 0, stop-start+1)
	currentIdx := int64(0)
	node := q.head

	for node != nil {
		nodeSize := int64(len(node.entries))

		if currentIdx+nodeSize > start && currentIdx <= stop {

			nodeStart := int64(0)
			if start > currentIdx {
				nodeStart = start - currentIdx
			}

			nodeStop := nodeSize - 1
			if stop < currentIdx+nodeSize-1 {
				nodeStop = stop - currentIdx
			}

			for i := nodeStart; i <= nodeStop; i++ {
				result = append(result, node.entries[i])
			}
		}

		currentIdx += nodeSize
		if currentIdx > stop {
			break
		}

		node = node.next
	}

	return result
}

func (q *Quicklist) Index(index int64) (string, bool) {
	if q.len == 0 {
		return "", false
	}

	if index < 0 {
		index = q.len + index
	}

	if index < 0 || index >= q.len {
		return "", false
	}

	currentIdx := int64(0)
	node := q.head

	for node != nil {
		nodeSize := int64(len(node.entries))

		if index < currentIdx+nodeSize {

			localIdx := index - currentIdx
			return node.entries[localIdx], true
		}

		currentIdx += nodeSize
		node = node.next
	}

	return "", false
}

func (q *Quicklist) canAddToNode(node *QuicklistNode, value string) bool {
	if len(node.entries) >= QuicklistNodeMaxEntries {
		return false
	}
	if node.size+len(value) > QuicklistNodeMaxSize {
		return false
	}
	return true
}

func (q *Quicklist) ToSlice() []string {
	if q.len == 0 {
		return []string{}
	}

	result := make([]string, 0, q.len)
	node := q.head

	for node != nil {
		result = append(result, node.entries...)
		node = node.next
	}

	return result
}
