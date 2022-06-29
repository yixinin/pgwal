package pgwal

import (
	"context"
	"os"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgproto3/v2"
)

func ReadWithTimeout(ctx context.Context, conn *pgconn.PgConn, timeout time.Duration) (pgproto3.BackendMessage, error) {
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(timeout))
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
