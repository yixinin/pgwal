package pgwal

import (
	"context"
	"os"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgproto3/v2"
)

func read(conn *pgconn.PgConn) (pgproto3.BackendMessage, error) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
	defer cancel()
	rawMsg, err := conn.ReceiveMessage(ctx)
	if err != nil {
		if pgconn.Timeout(err) {
			return nil, os.ErrDeadlineExceeded
		}
		return nil, err
	}
	return rawMsg, err
}
