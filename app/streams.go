package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type stream struct {
	first   [2]uint64
	last    [2]uint64
	entries []*streamEntry
	blocked []*chan bool
}

type streamEntry struct {
	id    [2]uint64
	store []string
}

func newStream() *stream {
	return &stream{
		first:   [2]uint64{0, 0},
		last:    [2]uint64{0, 0},
		entries: make([]*streamEntry, 0),
		blocked: make([]*chan bool, 0),
	}
}

func (s *stream) addStreamEntry(id string) (*streamEntry, error) {
	millisecondsTime, sequenceNumber, err := s.getNextID(id)
	if err != nil {
		return nil, err
	}

	if s.first[0] == 0 && s.first[1] == 0 {
		s.first[0], s.first[1] = millisecondsTime, sequenceNumber
	}
	s.last[0], s.last[1] = millisecondsTime, sequenceNumber

	entry := new(streamEntry)
	entry.id[0] = millisecondsTime
	entry.id[1] = sequenceNumber
	entry.store = make([]string, 0)
	s.entries = append(s.entries, entry)
	return entry, nil
}

func (s *stream) splitID(id string) (millisecondsTime, sequenceNumber uint64, hasSequence bool, err error) {
	parts := strings.Split(id, "-")
	millisecondsTime, err = strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return
	}
	if len(parts) > 1 {
		sequenceNumber, err = strconv.ParseUint(parts[1], 10, 64)
		hasSequence = true
	}
	return
}

func (s *stream) getNextID(id string) (millisecondsTime, sequenceNumber uint64, err error) {
	parts := strings.Split(id, "-")

	if len(parts) == 1 && parts[0] == "*" {
		millisecondsTime = uint64(time.Now().UnixMilli())
		if millisecondsTime == s.last[0] {
			sequenceNumber = s.last[1] + 1
		}
	} else if len(parts) == 2 && parts[1] == "*" {
		millisecondsTime, _ = strconv.ParseUint(parts[0], 10, 64)
		if millisecondsTime == s.last[0] {
			sequenceNumber = s.last[1] + 1
		} else if millisecondsTime > s.last[0] {
			sequenceNumber = 0
		} else {
			return 0, 0, fmt.Errorf("The ID specified in XADD is equal or smaller than the target stream top item")
		}
	} else {
		millisecondsTime, _ = strconv.ParseUint(parts[0], 10, 64)
		sequenceNumber, _ = strconv.ParseUint(parts[1], 10, 64)
	}

	if millisecondsTime == 0 && sequenceNumber == 0 {
		return 0, 0, fmt.Errorf("The ID specified in XADD must be greater than 0-0")
	}

	if millisecondsTime < s.last[0] || millisecondsTime == s.last[0] && sequenceNumber <= s.last[1] {
		return 0, 0, fmt.Errorf("The ID specified in XADD is equal or smaller than the target stream top item")
	}

	return
}

func searchStreamEntries(entries []*streamEntry, targetMs, targetSeq uint64, lo, hi int) int {
	for lo <= hi {
		mid := (lo + hi) / 2
		entry := entries[mid]
		if targetMs == entry.id[0] && targetSeq == entry.id[1] {
			lo = mid
			break
		} else if targetMs == entry.id[0] && entry.id[1] > targetSeq {
			hi = mid - 1
		} else if targetMs == entry.id[0] && entry.id[1] < targetSeq {
			lo = mid + 1
		} else if targetMs < entry.id[0] {
			hi = mid - 1
		} else {
			lo = mid + 1
		}
	}
	return lo
}
