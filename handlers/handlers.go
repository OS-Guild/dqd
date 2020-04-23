package handlers

import (
	"context"

	v1 "github.com/soluto/dqd/v1"
)

// Handler handles queue messages.
type Handler interface {
	Handle(context.Context, v1.Message) error
}

type FuncHandler func(context.Context, v1.Message) error

func (f FuncHandler) Handle(c context.Context, m v1.Message) error {
	return f(c, m)
}
