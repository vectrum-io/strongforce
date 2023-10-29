package events

import (
	"github.com/oklog/ulid/v2"
	"sort"
)

// EventID is the unique event identifier
type EventID string

func NewEventID() EventID {
	return EventID(ulid.Make().String())
}

func (i EventID) String() string {
	return string(i)
}

type EventIDSlice []EventID

func (x EventIDSlice) Len() int           { return len(x) }
func (x EventIDSlice) Less(i, j int) bool { return x[i] < x[j] }
func (x EventIDSlice) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

// Sort is a convenience method: x.Sort() calls Sort(x).
func (x EventIDSlice) Sort() { sort.Sort(x) }
