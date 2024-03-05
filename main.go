package main

import (
	"LogSender/internal/config"
	"LogSender/internal/fileutils"
	"LogSender/internal/storage"
	"LogSender/logger"
	"context"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"go.uber.org/zap"
	"io"
	"log"
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

	cons, err := fileutils.NewConsumer("log.txt")
	if err != nil {
		logger.Log.Error("failed to create consumer", zap.Error(err))
	}

	//считать последнее событие из бд
	db, err := sqlx.Open("postgres", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable")
	if err != nil {
		log.Println(err)
		return
	}

	if err := storage.Migrate(db); err != nil {
		log.Fatalln("Create migrate, err:", err)
	}

	s := storage.NewStorage(db)

	LastLog, err := s.ReadLastEvent(context.TODO())
	if err != nil {
		logger.Log.Error("failed to read last event", zap.Error(err))
		return
	}

	//проверяем файл нулевой ли
	sizeFile, err := cons.SizeFile()
	if err != nil {
		log.Println(err)
		return
	}

	if sizeFile != 0 {
		// поиск последнего события
		for {
			event, _, err := cons.ReadEvent()
			if err != nil {
				log.Println(err)
				break
			}
			if event.Ts > LastLog.Ts {
				//Отправка в бд

				err = s.WriteEvent(context.TODO(), logger.LoggerMsg{
					Level:        event.Level,
					Microservice: event.Microservice,
					Ts:           event.Ts,
					Caller:       event.Caller,
					Msg:          event.Msg,
					IdLogger:     event.IdLogger,
					Fields:       event.Fields,
					Error:        event.OriginalError,
				})
				if err != nil {
					logger.Log.Error("failed to write event", zap.Error(err))
					//break
				}

				break
			} else if event.Ts == LastLog.Ts || event.IdLogger == LastLog.IdLogger {
				//нашли ласт событие
				break
			}

		}
	}

	for {
		event, _, err := cons.ReadEvent()
		if err != nil {
			if err == io.EOF {
				continue
			}
			log.Println(err)
			return
		}
		//отправка события в бд
		err = s.WriteEvent(context.TODO(), logger.LoggerMsg{
			Level:        event.Level,
			Microservice: event.Microservice,
			Ts:           event.Ts,
			Caller:       event.Caller,
			Msg:          event.Msg,
			IdLogger:     event.IdLogger,
			Fields:       event.Fields,
			Error:        event.OriginalError,
		})
		if err != nil {
			logger.Log.Error("failed to write event", zap.Error(err))
			return
		}

	}
}
