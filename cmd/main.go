package main

import (
	"LogSender/internal/config"
	"LogSender/internal/fileutils"
	"LogSender/internal/service"
	"LogSender/internal/storage"
	"LogSender/logger"
	"context"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"go.uber.org/zap"
	"log"
	"sync"
	"time"
)

func run() error {
	flagLogLevel := "info"
	if err := logger.Initialize2(flagLogLevel, config.Get().PathFileLog, "LogSender"); err != nil {
		return err
	}
	return nil
}
func main() {
	if err := run(); err != nil {
		log.Fatalln(err)
		return
	}

	cons, err := fileutils.NewConsumer(config.Get().PathFileLog)
	if err != nil {
		logger.Log.Error("failed to create consumer", zap.Error(err))
	}

	//считать последнее событие из бд
	db, err := sqlx.Open("postgres", config.Get().DatabaseForLogDSN)
	if err != nil {
		logger.Log.Error("failed to open database", zap.Error(err))
		return
	}

	if err := storage.Migrate(db); err != nil {
		logger.Log.Fatal("failed to migrate", zap.Error(err))
	}

	//s := storage.NewStorage(db)

	Service := service.NewService(db)

	wg := sync.WaitGroup{}
	readlogger := service.NewReadLog(cons, make(chan logger.LoggerMsg), &wg, time.Duration(5*time.Second), *Service)
	readlogger.AddEventsToBuff(context.Background())
	readlogger.WriteEvents(context.Background())
	err = readlogger.ReadOldEvent(context.Background())
	if err != nil {
		logger.Log.Error("failed to read old event", zap.Error(err))
		return
	}

	readlogger.ReadNewEvent(context.Background())
	ctx := context.Background()
	<-ctx.Done()
}
