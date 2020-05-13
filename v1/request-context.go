package v1

import (
	"context"
	"time"
)

type contextKey string

func (c contextKey) String() string {
	return string(c)
}

var (
	ContextKeyMessage = contextKey("message")
	ContextKeyResult  = contextKey("result")
	ContextKeySource  = contextKey("source")
	ContextKeyStart   = contextKey("start")
)

type RequestContext struct {
	context.Context
}

func CreateRequestContext(ctx context.Context, source string, m Message) *RequestContext {
	return &RequestContext{
		context.WithValue(
			context.WithValue(
				context.WithValue(ctx,
					ContextKeySource, source),
				ContextKeyMessage, m),
			ContextKeyStart, time.Now()),
	}
}

func (r *RequestContext) Abort() bool {
	return r.Message().Abort()
}

func (r *RequestContext) Complete() error {
	return r.Message().Complete()
}

func (r *RequestContext) Message() Message {
	m, _ := r.Value(ContextKeyMessage).(Message)
	return m
}

func (r *RequestContext) Source() string {
	s, _ := r.Value(ContextKeySource).(string)
	return s
}

func (r *RequestContext) DequeueTime() time.Time {
	s, _ := r.Value(ContextKeyStart).(time.Time)
	return s
}

func (r *RequestContext) WithResult(m *RawMessage, err error) *RequestContext {
	if err == nil {
		return &RequestContext{context.WithValue(r.Context, ContextKeyResult, m)}
	} else {
		return &RequestContext{context.WithValue(r.Context, ContextKeyResult, err)}
	}
}

func (r *RequestContext) Result() (m *RawMessage, err error) {
	result := r.Value(ContextKeyResult)
	if result == nil {
		return nil, nil
	}
	if m, ok := result.(*RawMessage); ok {
		return m, nil
	}
	if err, ok := result.(error); ok {
		return nil, err
	}
	return nil, nil
}
