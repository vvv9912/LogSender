package storage

import (
	"LogSender/internal/storage/postgres"
	"LogSender/logger"
	"context"
	"github.com/jmoiron/sqlx"
)

type SandLogger interface {
	WriteEventBuf(ctx context.Context, event []logger.LoggerMsg) error
	WriteEvent(ctx context.Context, event logger.LoggerMsg) error
	ReadLastEvent(ctx context.Context) (*logger.LoggerMsg, error)
	ReadLastEventMicroservice(ctx context.Context, nameMicroservice string) (*logger.LoggerMsg, error)
}

type Storage struct {
	SandLogger
}

func NewStorage(db *sqlx.DB) *Storage {
	return &Storage{
		SandLogger: postgres.NewSandLogger(db),
	}
}
