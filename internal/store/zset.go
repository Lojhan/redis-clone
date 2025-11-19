package store

type ZSet struct {
	dict map[string]float64
	zsl  *skiplist
}

func NewZSet() *ZSet {
	return &ZSet{
		dict: make(map[string]float64),
		zsl:  newSkiplist(),
	}
}

func (zs *ZSet) Add(score float64, member string) bool {
	oldScore, exists := zs.dict[member]

	if exists {
		if oldScore != score {

			zs.zsl.delete(oldScore, member)
			zs.zsl.insert(score, member)
			zs.dict[member] = score
		}
		return false
	}

	zs.zsl.insert(score, member)
	zs.dict[member] = score
	return true
}

func (zs *ZSet) Remove(member string) bool {
	score, exists := zs.dict[member]
	if !exists {
		return false
	}

	zs.zsl.delete(score, member)
	delete(zs.dict, member)
	return true
}

func (zs *ZSet) Score(member string) (float64, bool) {
	score, exists := zs.dict[member]
	return score, exists
}

func (zs *ZSet) Card() int {
	return len(zs.dict)
}

func (zs *ZSet) Rank(member string) (int64, bool) {
	score, exists := zs.dict[member]
	if !exists {
		return -1, false
	}

	rank := zs.zsl.getRank(score, member)
	if rank == 0 {
		return -1, false
	}
	return rank - 1, true
}

func (zs *ZSet) Range(start, stop int64) []ZSetMember {

	nodes := zs.zsl.getRange(start+1, stop+1)

	result := make([]ZSetMember, len(nodes))
	for i, node := range nodes {
		result[i] = ZSetMember{
			Member: node.member,
			Score:  node.score,
		}
	}
	return result
}

type ZSetMember struct {
	Member string
	Score  float64
}
