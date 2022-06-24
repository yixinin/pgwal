package pgwal

import (
	"context"
	"fmt"

	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
)

// const outputPlugin = "wal2json"
const outputPlugin = "pgoutput"

func (r *Replica) CreateReplica(ctx context.Context, dsn string) error {
	conn, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	r.conn = conn

	var sql = fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", r.pubName)
	result := conn.Exec(ctx, sql)
	_, err = result.ReadAll()
	if err != nil {
		return err
	}
	sql = fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", r.pubName)
	result = conn.Exec(ctx, sql)
	_, err = result.ReadAll()
	if err != nil {
		return err
	}

	fmt.Println("created pub", r.pubName)
	sql = fmt.Sprintf("SELECT restart_lsn FROM pg_replication_slots WHERE slot_name='%s' and database='%s';", r.slotName, r.database)
	result = conn.Exec(ctx, sql)
	vals, err := result.ReadAll()
	if err != nil {
		return err
	}
	if len(vals) > 0 && len(vals[0].Rows) > 0 && len(vals[0].Rows[0]) > 0 {
		lsnStr := string(vals[0].Rows[0][0])
		if lsnStr != "" {
			lsn, err := pglogrepl.ParseLSN(lsnStr)
			if err != nil {
				return err
			}
			r.lsn = lsn
			fmt.Println("read old slot position")
			return nil
		}
	}

	// sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	// if err != nil {
	// 	return err
	// }
	// if sysident.XLogPos > pglogrepl.LSN(0) {
	// 	r.lsn = sysident.XLogPos
	// }

	var opts = pglogrepl.CreateReplicationSlotOptions{
		Temporary: false,
	}

	res, err := pglogrepl.CreateReplicationSlot(ctx, conn, r.slotName, outputPlugin, opts)
	if err != nil {
		fmt.Println(err)
		return err
	}

	lsn, err := pglogrepl.ParseLSN(res.ConsistentPoint)
	if err != nil {
		return err
	}
	r.lsn = lsn
	fmt.Println("created repl slot")

	return nil
}

func (r *Replica) Close(ctx context.Context) error {
	fmt.Println("call close")
	var err error
	r.once.Do(func() {
		if r.conn != nil {
			n, err := r.conn.Conn().Write([]byte{'X', 0, 0, 0, 4})
			fmt.Println("closing ...", n, err)
			// var opts = pglogrepl.DropReplicationSlotOptions{
			// 	Wait: true,
			// }
			// pglogrepl.DropReplicationSlot(ctx, r.conn, r.slotName, opts)
			err = r.conn.Close(ctx)
			fmt.Println("conn close", err)
		}
	})
	return err
}
