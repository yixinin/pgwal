package pgwal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
)

type Session struct {
	xid       *uint32
	relations map[uint32]*pglogrepl.RelationMessage

	outMsgPool sync.Pool

	pub Publisher
}

func NewSesstion(pub Publisher) *Session {
	return &Session{
		xid:       new(uint32),
		pub:       pub,
		relations: make(map[uint32]*pglogrepl.RelationMessage, 16),
		outMsgPool: sync.Pool{
			New: func() any {
				return &OutMessage{}
			},
		},
	}
}

func (sess *Session) HandleMessage(ctx context.Context, rawMsg pgproto3.BackendMessage) (ack bool, lsn pglogrepl.LSN, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic recovered err:%v, stacks:%s", r, debug.Stack())
		}
	}()
	if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
		return false, 0, fmt.Errorf("received Postgres WAL error: %+v", errMsg)
	}

	msg, ok := rawMsg.(*pgproto3.CopyData)
	if !ok {
		return false, 0, fmt.Errorf("received unexpected message: %T", rawMsg)
	}
	switch msg.Data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
		if err != nil {
			return false, 0, err
		}
		if pkm.ReplyRequested {
			return true, 0, nil
		}
	case pglogrepl.XLogDataByteID:
		xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
		if err != nil {
			return false, 0, err
		}
		logicalMsg, err := pglogrepl.Parse(xld.WALData)
		if err != nil {
			return false, 0, err
		}

		switch logicalMsg := logicalMsg.(type) {
		case *pglogrepl.RelationMessage:
			sess.relations[logicalMsg.RelationID] = logicalMsg
		case *pglogrepl.BeginMessage:
			// Indicates the beginning of a group of changes in a transaction. This is only sent for committed transactions. You won't get any events from rolled back transactions.
			*(sess.xid) = logicalMsg.Xid
		case *pglogrepl.CommitMessage:
		case *pglogrepl.InsertMessage:
			err = sess.handleInsert(ctx, logicalMsg, xld.ServerTime)
			if err != nil {
				return false, 0, nil
			}
		case *pglogrepl.UpdateMessage:
			err = sess.handleUpdate(ctx, logicalMsg, xld.ServerTime)
			if err != nil {
				return false, 0, nil
			}
		case *pglogrepl.DeleteMessage:
			err = sess.handleDelete(ctx, logicalMsg, xld.ServerTime)
			if err != nil {
				return false, 0, nil
			}
		case *pglogrepl.TruncateMessage:
		case *pglogrepl.TypeMessage:
		case *pglogrepl.OriginMessage:
		default:
			err = fmt.Errorf("unknown message type in pgoutput stream: %T", logicalMsg)
			return false, 0, err
		}
		lsn = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
		return true, lsn, nil
	}
	return
}

func (sess *Session) handleInsert(ctx context.Context, logicalMsg *pglogrepl.InsertMessage, commitTime time.Time) (err error) {
	rel, ok := sess.relations[logicalMsg.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation ID %d", logicalMsg.RelationID)
	}
	if logicalMsg.Tuple == nil {
		return errors.New("nil doc")
	}
	doc, err := decodeTuple(rel, logicalMsg.Tuple)
	outMsg := sess.outMsgPool.Get().(*OutMessage)
	defer func() {
		outMsg.Reset()
		sess.outMsgPool.Put(outMsg)
	}()
	outMsg.Table = fmt.Sprintf("%s.%s", rel.Namespace, rel.RelationName)
	outMsg.CommitTime = commitTime
	outMsg.New = doc
	outMsg.Action = "insert"
	outMsg.Xid = sess.xid

	buf, err := json.Marshal(outMsg)
	if err != nil {
		return
	}
	sess.pub.SendAsync(buf)
	return
}

func (sess *Session) handleDelete(ctx context.Context, logicalMsg *pglogrepl.DeleteMessage, commitTime time.Time) (err error) {
	rel, ok := sess.relations[logicalMsg.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation ID %d", logicalMsg.RelationID)
	}
	if logicalMsg.OldTuple == nil {
		return errors.New("nil doc")
	}
	doc, err := decodeTuple(rel, logicalMsg.OldTuple)
	outMsg := sess.outMsgPool.Get().(*OutMessage)
	defer func() {
		outMsg.Reset()
		sess.outMsgPool.Put(outMsg)
	}()
	outMsg.Table = fmt.Sprintf("%s.%s", rel.Namespace, rel.RelationName)
	outMsg.CommitTime = commitTime
	outMsg.Old = doc
	outMsg.Action = "delete"
	outMsg.Xid = sess.xid

	buf, err := json.Marshal(outMsg)
	if err != nil {
		return
	}
	sess.pub.SendAsync(buf)
	return
}

func (sess *Session) handleUpdate(ctx context.Context, logicalMsg *pglogrepl.UpdateMessage, commitTime time.Time) (err error) {
	rel, ok := sess.relations[logicalMsg.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation ID %d", logicalMsg.RelationID)
	}

	outMsg := sess.outMsgPool.Get().(*OutMessage)
	defer func() {
		outMsg.Reset()
		sess.outMsgPool.Put(outMsg)
	}()
	outMsg.Xid = sess.xid
	outMsg.Action = "update"
	outMsg.Table = fmt.Sprintf("%s.%s", rel.Namespace, rel.RelationName)
	outMsg.CommitTime = commitTime

	if logicalMsg.OldTuple != nil {
		doc, err := decodeTuple(rel, logicalMsg.OldTuple)
		if err != nil {
			return err
		}
		outMsg.Old = doc
	}
	if logicalMsg.NewTuple != nil {
		doc, err := decodeTuple(rel, logicalMsg.NewTuple)
		if err != nil {
			return err
		}
		outMsg.New = doc
	}

	buf, err := json.Marshal(outMsg)
	if err != nil {
		return err
	}
	sess.pub.SendAsync(buf)
	return
}
