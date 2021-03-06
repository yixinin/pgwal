package pgwal

import (
	"time"
)

type OutMessage struct {
	Table      string                 `json:"table"`
	Xid        uint32                 `json:"xid"`
	Old        map[string]interface{} `json:"old"`
	New        map[string]interface{} `json:"new"`
	Action     string                 `json:"action"`
	CommitTime time.Time              `json:"commitTime"`
}

func (m *OutMessage) Reset() {
	m.CommitTime = time.Time{}
	m.Table = ""
	m.New = nil
	m.Old = nil
}
