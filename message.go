package pgwal

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
)

var connInfo = pgtype.NewConnInfo()

func (r *Replica) handle(ctx context.Context, rawMsg pgproto3.BackendMessage) error {
	if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
		return fmt.Errorf("received Postgres WAL error: %+v", errMsg)
	}

	msg, ok := rawMsg.(*pgproto3.CopyData)
	if !ok {
		return fmt.Errorf("received unexpected message: %T", rawMsg)
	}

	switch msg.Data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
		if err != nil {
			return err
		}
		log.Println("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)
		if pkm.ReplyRequested {
			return r.SendStandbyState(ctx)
		}
	case pglogrepl.XLogDataByteID:
		xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
		if err != nil {
			return err
		}

		log.Println("XLogData =>", "WALStart", xld.WALStart, "ServerWALEnd", xld.ServerWALEnd, "ServerTime:", xld.ServerTime, "WALData", string(xld.WALData))
		logicalMsg, err := pglogrepl.Parse(xld.WALData)
		if err != nil {
			return err
		}
		log.Printf("Receive a logical replication message: %s", logicalMsg.Type())

		switch logicalMsg := logicalMsg.(type) {
		case *pglogrepl.RelationMessage:
			r.relations[logicalMsg.RelationID] = logicalMsg
		case *pglogrepl.BeginMessage:
			// Indicates the beginning of a group of changes in a transaction. This is only sent for committed transactions. You won't get any events from rolled back transactions.
			fmt.Println("[begin]", logicalMsg.FinalLSN, logicalMsg.Xid)
		case *pglogrepl.CommitMessage:
			fmt.Println("[commit]", logicalMsg.CommitLSN, logicalMsg.Flags, logicalMsg.TransactionEndLSN)
		case *pglogrepl.InsertMessage:
			rel, ok := r.relations[logicalMsg.RelationID]
			if !ok {
				return fmt.Errorf("unknown relation ID %d", logicalMsg.RelationID)
			}
			values := map[string]interface{}{}
			for idx, col := range logicalMsg.Tuple.Columns {
				colName := rel.Columns[idx].Name
				switch col.DataType {
				case 'n': // null
					values[colName] = nil
				case 'u': // unchanged toast
					// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
				case 't': //text
					val, err := decodeTextColumnData(connInfo, col.Data, rel.Columns[idx].DataType)
					if err != nil {
						return fmt.Errorf("error decoding column data: %w", err)
					}
					values[colName] = val
				}
			}

			jsonData, err := json.Marshal(values)
			if err != nil {
				return err
			}
			fmt.Println("[insert]", rel.RelationName, string(jsonData), err)

		case *pglogrepl.UpdateMessage:
			rel, ok := r.relations[logicalMsg.RelationID]
			if !ok {
				return fmt.Errorf("unknown relation ID %d", logicalMsg.RelationID)
			}
			if logicalMsg.OldTuple != nil {
				oldData, err := decodeTuple(rel, logicalMsg.OldTuple)
				fmt.Println("[update.old]", rel.RelationName, string(oldData), err)
			} else {
				fmt.Println("[update.old]", rel.RelationName, "nil")
			}
			if logicalMsg.NewTuple != nil {
				newData, err := decodeTuple(rel, logicalMsg.NewTuple)
				fmt.Println("[update.new]", rel.RelationName, string(newData), err)
			} else {
				fmt.Println("[update.new]", rel.RelationName, "nil")
			}

			// ...
		case *pglogrepl.DeleteMessage:
			rel, ok := r.relations[logicalMsg.RelationID]
			if !ok {
				return fmt.Errorf("unknown relation ID %d", logicalMsg.RelationID)
			}
			if logicalMsg.OldTuple != nil {
				oldData, err := decodeTuple(rel, logicalMsg.OldTuple)
				fmt.Println("[delete]", rel.RelationName, string(oldData), err)
			} else {
				fmt.Println("[delete]", rel.RelationName, "nil")
			}
			// ...
		case *pglogrepl.TruncateMessage:
			// ...

		case *pglogrepl.TypeMessage:
		case *pglogrepl.OriginMessage:
		default:
			return fmt.Errorf("unknown message type in pgoutput stream: %T", logicalMsg)
		}

		var oldLsn = r.lsn
		r.lsn = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
		if r.lsn > oldLsn {
			r.SendStandbyState(ctx)
		}
	}

	return nil
}

func decodeTextColumnData(ci *pgtype.ConnInfo, data []byte, dataType uint32) (interface{}, error) {
	var decoder pgtype.TextDecoder
	if dt, ok := ci.DataTypeForOID(dataType); ok {
		decoder, ok = dt.Value.(pgtype.TextDecoder)
		if !ok {
			decoder = &pgtype.GenericText{}
		}
	} else {
		decoder = &pgtype.GenericText{}
	}
	if err := decoder.DecodeText(ci, data); err != nil {
		return nil, err
	}
	return decoder.(pgtype.Value).Get(), nil
}

func decodeTuple(rel *pglogrepl.RelationMessage, data *pglogrepl.TupleData) ([]byte, error) {
	values := map[string]interface{}{}
	for idx, col := range data.Columns {
		colName := rel.Columns[idx].Name
		switch col.DataType {
		case 'n': // null
			values[colName] = nil
		case 'u': // unchanged toast
			// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
		case 't': //text
			val, err := decodeTextColumnData(connInfo, col.Data, rel.Columns[idx].DataType)
			if err != nil {
				return nil, err
			}
			values[colName] = val
		}
	}
	buf, err := json.Marshal(values)
	return buf, err
}
