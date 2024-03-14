package service

import (
	"LogSender/internal/storage/postgres"
	"LogSender/logger"
	"context"
	"github.com/jmoiron/sqlx"
)

type Database interface {
	WriteEventBuf(ctx context.Context, event []logger.LoggerMsg) error
	WriteEvent(ctx context.Context, event logger.LoggerMsg) error
	ReadLastEvent(ctx context.Context) (*logger.LoggerMsg, error)
	ReadLastEventMicroservice(ctx context.Context, nameMicroservice string) (*logger.LoggerMsg, error)
}

type Service struct {
	Database
}

func NewService(db *sqlx.DB) *Service {
	return &Service{
		Database: postgres.NewSandLogger(db),
	}
}
