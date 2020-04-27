package handlers

import (
	"context"

	v1 "github.com/soluto/dqd/v1"
)

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
	Handle(context.Context, v1.Message) HandlerError
}

type FuncHandler func(context.Context, v1.Message) HandlerError

func (f FuncHandler) Handle(c context.Context, m v1.Message) HandlerError {
	return f(c, m)
}

func WorkerHandler(h Handler) Handler {
	return FuncHandler(func(ctx context.Context, m v1.Message) HandlerError {
		err := h.Handle(ctx, m)
		if err != nil {
			if m.Retryable() {
				return nil
			} else {
				return err
			}
		}
		m.Done()
		return nil
	})
}
