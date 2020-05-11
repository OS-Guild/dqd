package handlers

import (
	"context"

	"github.com/rs/zerolog/log"
	v1 "github.com/soluto/dqd/v1"
)

var logger = log.With().Str("scope", "Handler").Logger()

type HandlerErrorCode int

type HandlerError interface {
	error
	Code() HandlerErrorCode
}

type handlerError struct {
	code  int
	error error
}

func (e *handlerError) Code() HandlerErrorCode {
	return HandlerErrorCode(e.code)
}

func (e *handlerError) Error() string {
	return e.error.Error()
}

func ServerError(err error) HandlerError {
	return &handlerError{
		5,
		err,
	}
}

func BadRequestError(err error) HandlerError {
	return &handlerError{
		4,
		err,
	}
}

// Handler handles queue messages.
type Handler interface {
	Handle(context.Context, v1.Message) (*v1.RawMessage, HandlerError)
}

type FuncHandler func(context.Context, v1.Message) (*v1.RawMessage, HandlerError)

func (f FuncHandler) Handle(c context.Context, m v1.Message) (*v1.RawMessage, HandlerError) {
	return f(c, m)
}

func WorkerHandler(h Handler, s v1.Source) Handler {
	return FuncHandler(func(ctx context.Context, m v1.Message) (*v1.RawMessage, HandlerError) {
		res, err := h.Handle(ctx, m)
		if err != nil {
			logger.Warn().Err(err).Msg("Error handling message")
			if m.Abort() {
				return nil, nil
			} else {
				return nil, err
			}
		}
		m.Done()
		return res, nil
	})
}
