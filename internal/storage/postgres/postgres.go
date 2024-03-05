package postgres

import (
	"LogSender/logger"
	"context"
	"github.com/jmoiron/sqlx"
)

type SandLogger struct {
	db *sqlx.DB
}

func NewSandLogger(db *sqlx.DB) *SandLogger {
	return &SandLogger{
		db: db,
	}
}

func (s *SandLogger) WriteEvent(ctx context.Context, event logger.LoggerMsg) error {
	_, err := s.db.QueryContext(ctx, "INSERT INTO events (level, microservice, ts, caller, msg, idLogger, fields, error) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
		event.Level, event.Microservice, event.Ts, event.Caller, event.Msg, event.IdLogger, event.Fields, event.Error)

	if err != nil {
		return err
	}
	return nil
}
func (s *SandLogger) ReadLastEvent(ctx context.Context) (*logger.LoggerMsg, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT level, ts, caller, msg, idLogger, fields, error, microservice FROM events ORDER BY id DESC LIMIT 1") //последнее событие
	if err != nil {
		return nil, err
	}

	var event logger.LoggerMsg

	for rows.Next() {
		if err = rows.Scan(
			&event.Level,
			&event.Ts,
			&event.Caller,
			&event.Msg,
			&event.IdLogger,
			&event.Fields,
			&event.Error,
			&event.Microservice); err != nil {
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

	var event logger.LoggerMsg

	for rows.Next() {
		if err = rows.Scan(
			&event.Level,
			&event.Ts,
			&event.Caller,
			&event.Msg,
			&event.IdLogger,
			&event.Fields,
			&event.Error,
			&event.Microservice); err != nil {
			return nil, err
		}

	}
	return &event, nil
}
