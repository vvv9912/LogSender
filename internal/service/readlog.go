package service

import (
	"LogSender/internal/fileutils"
	"LogSender/logger"
	"context"
	"go.uber.org/zap"
	"io"
	"sync"
	"time"
)

type ReadLog struct {
	consumer  *fileutils.Consumer
	buf       []logger.LoggerMsg
	mu        sync.Mutex
	wg        *sync.WaitGroup
	ch        chan logger.LoggerMsg
	timerSand time.Duration
	Service
}

func NewReadLog(consumer *fileutils.Consumer, ch chan logger.LoggerMsg, wg *sync.WaitGroup, timerSand time.Duration, Service Service) *ReadLog {
	return &ReadLog{
		consumer:  consumer,
		buf:       make([]logger.LoggerMsg, 0, 100),
		mu:        sync.Mutex{},
		wg:        wg,
		ch:        ch,
		timerSand: timerSand,
		Service:   Service,
	}
}

func (r *ReadLog) ReadOldEvent(ctx context.Context) error {
	LastLog, err := r.ReadLastEvent(ctx)
	if err != nil {
		logger.Log.Error("failed to read last event", zap.Error(err))
		return err
	}
	//проверяем файл нулевой ли
	sizeFile, err := r.consumer.SizeFile()
	if err != nil {
		logger.Log.Error("failed to get size file", zap.Error(err))
		return err
	}

	if sizeFile != 0 {
		// поиск последнего события
		for {
			event, _, err := r.consumer.ReadEvent()
			if err != nil {
				logger.Log.Error("failed to read event", zap.Error(err))
				break
			}
			if event.Ts > LastLog.Ts {
				//Отправка в бд
				r.ch <- logger.LoggerMsg{
					Level:        event.Level,
					Microservice: event.Microservice,
					Ts:           event.Ts,
					Caller:       event.Caller,
					Msg:          event.Msg,
					IdLogger:     event.IdLogger,
					Fields:       event.Fields,
					Error:        event.OriginalError,
				}
				//err = r.WriteEvent(context.TODO(), logger.LoggerMsg{
				//	Level:        event.Level,
				//	Microservice: event.Microservice,
				//	Ts:           event.Ts,
				//	Caller:       event.Caller,
				//	Msg:          event.Msg,
				//	IdLogger:     event.IdLogger,
				//	Fields:       event.Fields,
				//	Error:        event.OriginalError,
				//})
				//if err != nil {
				//	logger.Log.Error("failed to write event", zap.Error(err))
				//	//break
				//}

				break
			} else if event.Ts == LastLog.Ts || event.IdLogger == LastLog.IdLogger {
				//нашли ласт событие
				break
			}

		}
	}
	return nil
}

func (r *ReadLog) ReadNewEvent(ctx context.Context) error {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				event, _, err := r.consumer.ReadEvent()
				if err != nil {
					if err == io.EOF {
						continue
					}
					logger.Log.Error("failed to read event", zap.Error(err))
					return
				}
				//отправка события в бд
				r.ch <- logger.LoggerMsg{
					Level:        event.Level,
					Microservice: event.Microservice,
					Ts:           event.Ts,
					Caller:       event.Caller,
					Msg:          event.Msg,
					IdLogger:     event.IdLogger,
					Fields:       event.Fields,
					Error:        event.OriginalError,
				}

			}
		}
	}()
	return nil
}

func (r *ReadLog) AddEventsToBuff(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case event := <-r.ch:
				r.mu.Lock()
				if r.buf == nil {
					r.buf = make([]logger.LoggerMsg, 0)
				}
				r.buf = append(r.buf, event)
				r.wg.Add(1)
				r.mu.Unlock()

				if len(r.buf) >= 10 {
					r.wg.Wait()
				}
			}
		}
	}()
}

func (r *ReadLog) WriteEvents(ctx context.Context) {
	go func() {
		timer := time.NewTicker(5 * time.Second)
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				r.mu.Lock()

				if len(r.buf) == 0 {
					r.mu.Unlock()
					continue
				}

				err := r.WriteEventBuf(ctx, r.buf)
				if err != nil {
					//todo в случае если бд будет не доступно, сервис упадет //горутина с передподключением и общим флагом отправки в бд?
					r.mu.Unlock()
					return
				}

				for range r.buf {
					r.wg.Done()
				}

				r.buf = r.buf[:0]

				r.mu.Unlock()
			}
		}
	}()
}
