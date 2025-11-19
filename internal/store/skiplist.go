package store

import (
	"math/rand"
)

const (
	skiplistMaxLevel = 32
	skiplistP        = 0.25
)

type skiplistNode struct {
	member string
	score  float64
	level  []*skiplistLevel
}

type skiplistLevel struct {
	forward *skiplistNode
	span    int64
}

type skiplist struct {
	header *skiplistNode
	tail   *skiplistNode
	length int64
	level  int
}

func newSkiplist() *skiplist {
	sl := &skiplist{
		level: 1,
	}
	sl.header = &skiplistNode{
		level: make([]*skiplistLevel, skiplistMaxLevel),
	}
	for i := 0; i < skiplistMaxLevel; i++ {
		sl.header.level[i] = &skiplistLevel{}
	}
	return sl
}

func randomLevel() int {
	level := 1
	for level < skiplistMaxLevel && rand.Float64() < skiplistP {
		level++
	}
	return level
}

func (sl *skiplist) insert(score float64, member string) *skiplistNode {
	update := make([]*skiplistNode, skiplistMaxLevel)
	rank := make([]int64, skiplistMaxLevel)

	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		if i == sl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}

		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				(x.level[i].forward.score == score && x.level[i].forward.member < member)) {
			rank[i] += x.level[i].span
			x = x.level[i].forward
		}
		update[i] = x
	}

	level := randomLevel()
	if level > sl.level {
		for i := sl.level; i < level; i++ {
			rank[i] = 0
			update[i] = sl.header
			update[i].level[i].span = sl.length
		}
		sl.level = level
	}

	x = &skiplistNode{
		score:  score,
		member: member,
		level:  make([]*skiplistLevel, level),
	}
	for i := 0; i < level; i++ {
		x.level[i] = &skiplistLevel{}
		x.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = x

		x.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	for i := level; i < sl.level; i++ {
		update[i].level[i].span++
	}

	if update[0] != sl.header {

	}

	if x.level[0].forward == nil {
		sl.tail = x
	}

	sl.length++
	return x
}

func (sl *skiplist) delete(score float64, member string) bool {
	update := make([]*skiplistNode, skiplistMaxLevel)

	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				(x.level[i].forward.score == score && x.level[i].forward.member < member)) {
			x = x.level[i].forward
		}
		update[i] = x
	}

	x = x.level[0].forward
	if x != nil && x.score == score && x.member == member {
		sl.deleteNode(x, update)
		return true
	}
	return false
}

func (sl *skiplist) deleteNode(x *skiplistNode, update []*skiplistNode) {
	for i := 0; i < sl.level; i++ {
		if update[i].level[i].forward == x {
			update[i].level[i].span += x.level[i].span - 1
			update[i].level[i].forward = x.level[i].forward
		} else {
			update[i].level[i].span--
		}
	}

	if x.level[0].forward != nil {

	} else {
		sl.tail = update[0]
	}

	for sl.level > 1 && sl.header.level[sl.level-1].forward == nil {
		sl.level--
	}
	sl.length--
}

func (sl *skiplist) getRank(score float64, member string) int64 {
	rank := int64(0)
	x := sl.header

	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				(x.level[i].forward.score == score && x.level[i].forward.member <= member)) {
			rank += x.level[i].span
			x = x.level[i].forward
		}

		if x.member == member && x.score == score {
			return rank
		}
	}
	return 0
}

func (sl *skiplist) getByRank(rank int64) *skiplistNode {
	if rank <= 0 || rank > sl.length {
		return nil
	}

	traversed := int64(0)
	x := sl.header

	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && (traversed+x.level[i].span) <= rank {
			traversed += x.level[i].span
			x = x.level[i].forward
		}

		if traversed == rank {
			return x
		}
	}
	return nil
}

func (sl *skiplist) getRange(start, stop int64) []*skiplistNode {
	if start <= 0 || start > sl.length {
		return nil
	}
	if stop <= 0 || stop > sl.length {
		stop = sl.length
	}
	if start > stop {
		return nil
	}

	result := make([]*skiplistNode, 0, stop-start+1)
	traversed := int64(0)
	x := sl.header

	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && traversed+x.level[i].span < start {
			traversed += x.level[i].span
			x = x.level[i].forward
		}
	}

	x = x.level[0].forward
	traversed++

	for traversed <= stop && x != nil {
		result = append(result, x)
		x = x.level[0].forward
		traversed++
	}

	return result
}

func (sl *skiplist) first() *skiplistNode {
	return sl.header.level[0].forward
}
