package store

type Set struct {
	members map[string]struct{}
}

func NewSet() *Set {
	return &Set{
		members: make(map[string]struct{}),
	}
}

func (s *Set) Add(members ...string) int {
	added := 0
	for _, member := range members {
		if _, exists := s.members[member]; !exists {
			s.members[member] = struct{}{}
			added++
		}
	}
	return added
}

func (s *Set) Remove(members ...string) int {
	removed := 0
	for _, member := range members {
		if _, exists := s.members[member]; exists {
			delete(s.members, member)
			removed++
		}
	}
	return removed
}

func (s *Set) IsMember(member string) bool {
	_, exists := s.members[member]
	return exists
}

func (s *Set) Members() []string {
	result := make([]string, 0, len(s.members))
	for member := range s.members {
		result = append(result, member)
	}
	return result
}

func (s *Set) Card() int {
	return len(s.members)
}

func (s *Set) Pop() (string, bool) {
	if len(s.members) == 0 {
		return "", false
	}

	for member := range s.members {
		delete(s.members, member)
		return member, true
	}

	return "", false
}
