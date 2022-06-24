package pgwal

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
)

// var pluginArguments = []string{"\"pretty-print\" 'true'"}

type Replica struct {
	once            sync.Once
	pluginArguments []string

	pubName  string
	slotName string
	database string
	// clientXLogPos pglogrepl.LSN

	conn *pgconn.PgConn
	// sysident  pglogrepl.IdentifySystemResult
	relations map[uint32]*pglogrepl.RelationMessage
	lsn       pglogrepl.LSN
}

func NewReplica(pubName, slotName, database string) *Replica {
	r := &Replica{
		pubName:         pubName,
		slotName:        slotName,
		database:        database,
		relations:       make(map[uint32]*pglogrepl.RelationMessage, 16),
		pluginArguments: []string{"proto_version '1'", fmt.Sprintf("publication_names '%s'", pubName)},
	}
	return r
}

func (r *Replica) Run(ctx context.Context) error {
	var opts = pglogrepl.StartReplicationOptions{
		PluginArgs: r.pluginArguments,
	}
	err := pglogrepl.StartReplication(ctx, r.conn, r.slotName, r.lsn, opts)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			// exit
			return nil
		default:
		}
		rawMsg, err := read(r.conn)
		if os.IsTimeout(err) {
			r.SendStandbyState(ctx)
			continue
		}
		if err != nil {
			return err
		}

		err = r.handle(ctx, rawMsg)
		if err != nil {
			return err
		}
	}
}
func (r *Replica) SendStandbyState(ctx context.Context) error {
	var update = pglogrepl.StandbyStatusUpdate{WALWritePosition: r.lsn}
	err := pglogrepl.SendStandbyStatusUpdate(ctx, r.conn, update)
	return err
}
