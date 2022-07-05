package pgwal

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pglogrepl"
)

// const outputPlugin = "wal2json"
const outputPlugin = "pgoutput"

func (r *Replication) CreatePublication(ctx context.Context, tables ...string) error {
	var sql = fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", r.opts.PublicationName())
	result := r.conn.Exec(ctx, sql)
	_, err := result.ReadAll()
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		sql = fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", r.opts.PublicationName())
	} else {
		sql = fmt.Sprintf("CREATE PUBLICATION %s FOR Table %s", r.opts.PublicationName(), strings.Join(tables, ","))
	}

	result = r.conn.Exec(ctx, sql)
	_, err = result.ReadAll()
	return err
}

func (r *Replication) CreateOrLoadReplicaSlot(ctx context.Context) error {
	sql := fmt.Sprintf("SELECT restart_lsn FROM pg_replication_slots WHERE slot_name='%s' and database='%s';", r.opts.SlotName(), r.opts.Database)
	result := r.conn.Exec(ctx, sql)
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
			return nil
		}
	}

	var opts = pglogrepl.CreateReplicationSlotOptions{}
	res, err := pglogrepl.CreateReplicationSlot(ctx, r.conn, r.opts.SlotName(), outputPlugin, opts)
	if err != nil {
		return err
	}

	lsn, err := pglogrepl.ParseLSN(res.ConsistentPoint)
	if err != nil {
		return err
	}
	r.lsn = lsn
	return nil
}

func (r *Replication) Close(ctx context.Context) error {
	var err error
	r.once.Do(func() {
		if r.conn != nil {
			_, _ = r.conn.Conn().Write([]byte{'X', 0, 0, 0, 4})
			_ = r.conn.Close(ctx)
		}
	})
	return err
}

func (r *Replication) SetTableReplicaIdentity(ctx context.Context, tables []string) error {
	for _, v := range tables {
		var query = fmt.Sprintf("alter table %s replica identity full;", v)
		_, err := r.conn.Exec(ctx, query).ReadAll()
		if err != nil {
			return err
		}
	}
	return nil
}
