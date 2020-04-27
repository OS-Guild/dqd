package handlers

import (
	"context"

	v1 "github.com/soluto/dqd/v1"
)

func NewWriteToSourceErrorHandler(handler Handler, source v1.Source) Handler {
	p := source.CreateProducer()
	return FuncHandler(func(ctx context.Context, m v1.Message) HandlerError {
		err := handler.Handle(ctx, m)
		if err != nil {
			if p.Produce(ctx, v1.RawMessage{
				Data: m.Data(),
			}) != nil {
				return err
			}
		}
		m.Done()
		return nil
	})
}
