package fileutils

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"path"
)

type Event struct {
	Level         string  `json:"level"`
	Microservice  string  `json:"microservice"`
	Ts            float64 `json:"ts"`
	Caller        string  `json:"caller"`
	Msg           string  `json:"msg"`
	IdLogger      string  `json:"idLogger"`
	Fields        string  `json:"fields"`
	OriginalError string  `json:"error"`
}

type Producer struct {
	file    *os.File
	encoder *json.Encoder
}

func NewProducer(fileName string) (*Producer, error) {
	if fileName[0] == '/' {
		fileName = fileName[1:]
	}
	err := os.MkdirAll(path.Dir(fileName), os.ModePerm)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	return &Producer{
		file:    file,
		encoder: json.NewEncoder(file),
	}, nil
}

func (p *Producer) WriteEvent(event *Event) error {
	return p.encoder.Encode(&event)
}

func (p *Producer) Close() error {
	return p.file.Close()
}

type Consumer struct {
	file    *os.File
	decoder *json.Decoder
	reader  *bufio.Reader
}

func NewConsumer(fileName string) (*Consumer, error) {
	file, err := os.OpenFile(fileName, os.O_RDONLY|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	reader := bufio.NewReader(file)
	return &Consumer{
		file:    file,
		decoder: json.NewDecoder(file),
		reader:  reader,
	}, nil
}

func (c *Consumer) Seek(offset int64, whence int) (int64, error) {
	return c.file.Seek(offset, whence)

}
func (c *Consumer) SeekEvent(offset int64, whence int) (int64, error) {

	return c.file.Seek(offset, whence)

}
func (c *Consumer) ReadEvent() (*Event, int64, error) {
	event := &Event{}
	if err := c.decoder.Decode(&event); err != nil {
		return nil, 0, err
	}
	ofset := c.decoder.InputOffset()

	return event, ofset, nil
}

func (c *Consumer) SizeFile() (int64, error) {
	fileInfo, err := c.file.Stat()
	if err != nil {
		return 0, err
	}
	return fileInfo.Size(), nil
}

func (c *Consumer) ReadEvents() (*Event, int64, error) {
	event := &Event{}

	line, err := c.reader.ReadBytes('\n')
	if err != nil {
		return nil, 0, err
	}
	if err := json.Unmarshal(line, &event); err != nil {
		return nil, 0, err
	}
	lastPosition, _ := c.file.Seek(0, io.SeekCurrent)

	return event, lastPosition, nil
}

func (c *Consumer) ReaderEvent(file string) (*Event, int64, error) {
	event := &Event{}
	if err := c.decoder.Decode(&event); err != nil {
		return nil, 0, err
	}
	ofset := c.decoder.InputOffset()

	return event, ofset, nil
}

// func (c *Consumer) ReadLastEvent(fileName string) (*Event, error) {
//
//		var lastEvent *Event
//		for {
//			event, err := c.ReadEvent()
//			if err != nil {
//				if err == io.EOF { // Достигнут конец файла
//					break
//				}
//				return nil, err
//			}
//			lastEvent = event
//		}
//
//		return lastEvent, nil
//	}
func (c *Consumer) Close() error {
	return c.file.Close()
}
