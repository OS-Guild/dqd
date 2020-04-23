package handlers

import (
	"context"

	v1 "github.com/soluto/dqd/v1"
)

func NewWriteToSourceErrorHandler(handler Handler, source v1.Source) Handler {
	p := source.CreateProducer()
	return FuncHandler(func(ctx context.Context, m v1.Message) error {
		err := handler.Handle(ctx, m)
		if err != nil {
			return err
		}
		return p.Produce(ctx, v1.RawMessage{
			Data: m.Data(),
		})
	})
}
