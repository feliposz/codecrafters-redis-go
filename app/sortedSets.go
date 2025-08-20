package main

import (
	"cmp"
	"slices"
)

type sortedSetContainer struct {
	members map[string]*sortedSetEntry
	sorted  []*sortedSetEntry
}

type sortedSetEntry struct {
	score  float64
	member string
}

func sortedSetEntryCompare(a, b *sortedSetEntry) int {
	if a.score == b.score {
		return cmp.Compare(a.member, b.member)
	}
	return cmp.Compare(a.score, b.score)
}

func NewSortedSet() *sortedSetContainer {
	set := &sortedSetContainer{}
	set.members = make(map[string]*sortedSetEntry)
	return set
}

func (s *sortedSetContainer) Put(score float64, member string) int {
	newEntry := &sortedSetEntry{score, member}
	count := 1
	if oldEntry, exists := s.members[member]; exists {
		pos, _ := slices.BinarySearchFunc(s.sorted, oldEntry, sortedSetEntryCompare)
		s.sorted = slices.Delete(s.sorted, pos, pos)
		count = 0
	}
	pos, _ := slices.BinarySearchFunc(s.sorted, newEntry, sortedSetEntryCompare)
	s.sorted = slices.Insert(s.sorted, pos, newEntry)
	s.members[member] = newEntry
	return count
}

func (s *sortedSetContainer) GetRank(member string) int {
	if oldEntry, exists := s.members[member]; exists {
		pos, _ := slices.BinarySearchFunc(s.sorted, oldEntry, sortedSetEntryCompare)
		return pos
	}
	return -1
}
