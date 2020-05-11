package pipe

import (
	"context"
	"time"

	v1 "github.com/soluto/dqd/v1"
)

type contextKey string

func (c contextKey) String() string {
	return string(c)
}

var (
	contextKeyMessage = contextKey("message")
	contextKeyResult  = contextKey("result")
	contextKeySource  = contextKey("source")
	contextKeyStart   = contextKey("start")
)

type requestContext struct {
	context.Context
}

func createRequestContext(ctx context.Context, source string, m v1.Message) *requestContext {
	return &requestContext{
		context.WithValue(
			context.WithValue(
				context.WithValue(ctx,
					contextKeySource, source),
				contextKeyMessage, m),
			contextKeyStart, time.Now()),
	}
}

func (r *requestContext) Message() v1.Message {
	m, _ := r.Value(contextKeySource).(v1.Message)
	return m
}

func (r *requestContext) Source() string {
	s, _ := r.Value(contextKeySource).(string)
	return s
}

func (r *requestContext) StartTime() time.Time {
	s, _ := r.Value(contextKeyStart).(time.Time)
	return s
}

func (r *requestContext) WithResult(m *v1.RawMessage, err error) *requestContext {
	if err == nil {
		return &requestContext{context.WithValue(r.Context, contextKeyResult, m)}
	} else {
		return &requestContext{context.WithValue(r.Context, contextKeyResult, err)}
	}
}

func (r *requestContext) Result() (m *v1.RawMessage, err error) {
	result := r.Value("result")
	if result == nil {
		return nil, nil
	}
	if m, ok := result.(*v1.RawMessage); ok {
		return m, nil
	}
	if err, ok := result.(error); ok {
		return nil, err
	}
	return nil, nil
}
