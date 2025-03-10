package utils

import (
	"context"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/Laisky/zap"
	"github.com/pkg/errors"
)

const (
	defaultEventEngineNFork         int = 2
	defaultEventEngineMsgBufferSize int = 1
)

// EventTopic topic of event
type EventTopic string

func (h EventTopic) String() string {
	return string(h)
}

// HandlerID id(name) of event handler
type HandlerID string

func (h HandlerID) String() string {
	return string(h)
}

// MetaKey key of event's meta
type MetaKey string

func (h MetaKey) String() string {
	return string(h)
}

// EventMeta event meta
type EventMeta map[MetaKey]interface{}

// Event evt
type Event struct {
	Topic EventTopic
	Time  time.Time
	Meta  EventMeta
	Stack string
}

// EventHandler function to handle event
type EventHandler func(*Event)

// EventEngine type of event store
type EventEngine struct {
	*eventStoreManagerOpt
	q chan *Event

	// topic2hs map[topic]*sync.Map[handlerID]handler
	topic2hs *sync.Map
}

type eventStoreManagerOpt struct {
	msgBufferSize int
	nfork         int
	logger        *LoggerType
	suppressPanic bool
}

// EventEngineOptFunc options for EventEngine
type EventEngineOptFunc func(*eventStoreManagerOpt) error

// WithEventEngineNFork set nfork of event store
func WithEventEngineNFork(nfork int) EventEngineOptFunc {
	return func(opt *eventStoreManagerOpt) error {
		if nfork <= 0 {
			return errors.Errorf("nfork must > 0")
		}

		opt.nfork = nfork
		return nil
	}
}

// WithEventEngineChanBuffer set msg buffer size of event store
func WithEventEngineChanBuffer(msgBufferSize int) EventEngineOptFunc {
	return func(opt *eventStoreManagerOpt) error {
		if msgBufferSize < 0 {
			return errors.Errorf("msgBufferSize must >= 0")
		}

		opt.msgBufferSize = msgBufferSize
		return nil
	}
}

// WithEventEngineLogger set event store's logger
func WithEventEngineLogger(logger *LoggerType) EventEngineOptFunc {
	return func(opt *eventStoreManagerOpt) error {
		if logger == nil {
			return errors.Errorf("logger is nil")
		}

		opt.logger = logger
		return nil
	}
}

// WithEventEngineSuppressPanic set whether suppress event handler's panic
func WithEventEngineSuppressPanic(suppressPanic bool) EventEngineOptFunc {
	return func(opt *eventStoreManagerOpt) error {
		opt.suppressPanic = suppressPanic
		return nil
	}
}

// NewEventEngine new event store manager
func NewEventEngine(ctx context.Context, opts ...EventEngineOptFunc) (e *EventEngine, err error) {
	opt := &eventStoreManagerOpt{
		msgBufferSize: defaultEventEngineMsgBufferSize,
		nfork:         defaultEventEngineNFork,
		logger:        Logger.Named("evt-store-" + RandomStringWithLength(6)),
	}
	for _, optf := range opts {
		if err = optf(opt); err != nil {
			return nil, err
		}
	}

	e = &EventEngine{
		eventStoreManagerOpt: opt,
		q:                    make(chan *Event, opt.msgBufferSize),
		topic2hs:             &sync.Map{},
	}

	taskChan := make(chan *eventRunChanItem, opt.msgBufferSize)
	e.startRunner(ctx, opt.nfork, taskChan)
	e.run(ctx, taskChan)
	e.logger.Info("new event store",
		zap.Int("nfork", opt.nfork),
		zap.Int("buffer", opt.msgBufferSize))
	return e, nil
}

func runHandlerWithoutPanic(h EventHandler, evt *Event) (err error) {
	defer func() {
		if erri := recover(); erri != nil {
			err = errors.Errorf("run event handler with evt `%s`: %+v", evt.Topic, erri)
		}
	}()

	h(evt)
	return nil
}

type eventRunChanItem struct {
	h   EventHandler
	hid HandlerID
	evt *Event
}

func (e *EventEngine) startRunner(ctx context.Context, nfork int, taskChan chan *eventRunChanItem) {
	for i := 0; i < nfork; i++ {
		logger := e.logger.Named(strconv.Itoa(i))
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case t := <-taskChan:
					logger.Debug("trigger handler",
						zap.String("evt", t.evt.Topic.String()),
						zap.String("handler", t.hid.String()))

					if e.suppressPanic {
						if err := runHandlerWithoutPanic(t.h, t.evt); err != nil {
							logger.Error("handler panic",
								zap.String("handler", t.hid.String()),
								zap.String("stack", t.evt.Stack),
								zap.Error(err))
						}
					} else {
						t.h(t.evt)
					}
				}
			}
		}()
	}
}

// Run start EventEngine
func (e *EventEngine) run(ctx context.Context, taskChan chan *eventRunChanItem) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case evt := <-e.q:
				hsi, ok := e.topic2hs.Load(evt.Topic)
				if !ok || hsi == nil {
					continue
				}

				hsi.(*sync.Map).Range(func(hid, h interface{}) bool {
					taskChan <- &eventRunChanItem{
						h:   h.(EventHandler),
						hid: hid.(HandlerID),
						evt: evt,
					}

					return true
				})
			}
		}
	}()
}

// Register register new handler to event store
func (e *EventEngine) Register(topic EventTopic, handlerID HandlerID, handler EventHandler) {
	hs := &sync.Map{}
	actual, _ := e.topic2hs.LoadOrStore(topic, hs)
	actual.(*sync.Map).Store(handlerID, handler)

	e.logger.Info("register handler",
		zap.String("topic", topic.String()),
		zap.String("handler", handlerID.String()))
}

// UnRegister delete handler in event store
func (e *EventEngine) UnRegister(topic EventTopic, handlerID HandlerID) {
	if hsi, _ := e.topic2hs.Load(topic); hsi != nil {
		hsi.(*sync.Map).Delete(handlerID)
	}

	e.logger.Info("unregister handler",
		zap.String("topic", topic.String()),
		zap.String("handler", handlerID.String()))
}

// Publish publish new event
func (e *EventEngine) Publish(evt *Event) {
	evt.Time = Clock.GetUTCNow()
	evt.Stack = string(debug.Stack())
	e.q <- evt
	e.logger.Debug("publish event", zap.String("event", evt.Topic.String()))
}
