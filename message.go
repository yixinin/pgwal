package pgwal

import (
	"context"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgtype"
)

type OutMessage struct {
	Table      string                 `json:"table"`
	Old        map[string]interface{} `json:"old"`
	New        map[string]interface{} `json:"new"`
	Action     string                 `json:"action"`
	CommitTime time.Time              `json:"commit_time"`
}

func (m *OutMessage) Reset() {
	m.CommitTime = time.Time{}
	m.Table = ""
	m.New = nil
	m.Old = nil
}

var connInfo = pgtype.NewConnInfo()

func (r *Replication) SendStandbyState(ctx context.Context) error {
	var update = pglogrepl.StandbyStatusUpdate{WALWritePosition: r.lsn}
	err := pglogrepl.SendStandbyStatusUpdate(ctx, r.conn, update)
	return err
}
