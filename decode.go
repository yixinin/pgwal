package pgwal

import (
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgtype"
)

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

func decodeTuple(rel *pglogrepl.RelationMessage, data *pglogrepl.TupleData) (map[string]interface{}, error) {
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
	return values, nil
}
