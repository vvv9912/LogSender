package postgres

import (
	"LogSender/logger"
	"context"
	"encoding/json"
	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"
)

type SandLogger struct {
	db *sqlx.DB
}

func NewSandLogger(db *sqlx.DB) *SandLogger {
	return &SandLogger{
		db: db,
	}
}
func (s *SandLogger) WriteEventBuf(ctx context.Context, event []logger.LoggerMsg) error {
	tx, err := s.db.Beginx()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()

	for _, v := range event {
		err = s.writeEvent(ctx, tx, v)
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *SandLogger) WriteEvent(ctx context.Context, event logger.LoggerMsg) error {
	tx, err := s.db.Beginx()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()
	return s.writeEvent(ctx, tx, event)
}
func (s *SandLogger) writeEvent(ctx context.Context, tx *sqlx.Tx, event logger.LoggerMsg) error {
	fields, err := json.Marshal(event.Fields)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO events (level, microservice, ts, caller, msg, idLogger, fields, error) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
		event.Level, event.Microservice, event.Ts, event.Caller, event.Msg, event.IdLogger, string(fields), event.Error)

	if err != nil {
		logger.Log.Error("failed to write event", zap.Error(err))
		return err
	}
	return nil
}
func (s *SandLogger) ReadLastEvent(ctx context.Context) (*logger.LoggerMsg, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT level, ts, caller, msg, idLogger, fields, error, microservice FROM events ORDER BY id DESC LIMIT 1") //последнее событие
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var event logger.LoggerMsg
	var fields string
	for rows.Next() {
		if err = rows.Scan(
			&event.Level,
			&event.Ts,
			&event.Caller,
			&event.Msg,
			&event.IdLogger,
			&fields,
			&event.Error,
			&event.Microservice); err != nil {
			return nil, err
		}

	}
	if fields != "" {
		err = json.Unmarshal([]byte(fields), &event.Fields)
		if err != nil {
			return nil, err
		}
	}
	return &event, nil
}

// последнее событие конкретного микросервиса
func (s *SandLogger) ReadLastEventMicroservice(ctx context.Context, nameMicroservice string) (*logger.LoggerMsg, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT * FROM events where microservice = $1 ORDER BY id DESC LIMIT 1", nameMicroservice)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var event logger.LoggerMsg
	var fields string
	for rows.Next() {
		if err = rows.Scan(
			&event.Level,
			&event.Ts,
			&event.Caller,
			&event.Msg,
			&event.IdLogger,
			&fields,
			&event.Error,
			&event.Microservice); err != nil {
			return nil, err
		}

	}
	if fields != "" {
		err = json.Unmarshal([]byte(fields), &event.Fields)
		if err != nil {
			return nil, err
		}
	}
	return &event, nil
}
