package pgwal

import (
	"context"
	"fmt"
	"os"
	"runtime/debug"
	"sync"

	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
)

// var pluginArguments = []string{"\"pretty-print\" 'true'"}

type Replication struct {
	once sync.Once
	opts *Options

	pub  Publisher
	conn *pgconn.PgConn

	lsn pglogrepl.LSN
}

func NewReplica(pub Publisher, opts *Options) *Replication {
	r := &Replication{
		pub:  pub,
		opts: opts,
	}
	return r
}

func (r *Replication) Run(ctx context.Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r, string(debug.Stack()))
		}
		if err != nil {
			fmt.Println(err)
		}
	}()

	conn, err := pgconn.Connect(ctx, r.opts.Dsn())
	if err != nil {
		return err
	}
	r.conn = conn

	if err = r.CreateOrLoadReplicaSlot(ctx); err != nil {
		return err
	}

	var opts = pglogrepl.StartReplicationOptions{
		PluginArgs: r.opts.PluginArguments(),
	}
	err = pglogrepl.StartReplication(ctx, r.conn, r.opts.SlotName(), r.lsn, opts)
	if err != nil {
		return err
	}
	var sesstion = NewSesstion(r.pub)
	for {
		select {
		case <-ctx.Done():
			// exit
			return nil
		default:
		}
		rawMsg, err := read(ctx, r.conn, r.opts.ReadTimeout)
		if os.IsTimeout(err) {
			r.SendStandbyState(ctx)
			continue
		}
		if err != nil {
			return err
		}

		ack, lsn, err := sesstion.HandleMessage(ctx, rawMsg)
		if err != nil {
			return err
		}
		if lsn > 0 {
			r.lsn = lsn
		}
		if ack {
			if err := r.SendStandbyState(ctx); err != nil {
				return err
			}
		}
	}
}

func (r *Replication) SendStandbyState(ctx context.Context) error {
	var update = pglogrepl.StandbyStatusUpdate{WALWritePosition: r.lsn}
	err := pglogrepl.SendStandbyStatusUpdate(ctx, r.conn, update)
	return err
}
