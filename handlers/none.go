package handlers

import (
	v1 "github.com/soluto/dqd/v1"
)

type noneHandler struct {
}

func (h *noneHandler) Handle(ctx *v1.RequestContext, message v1.Message) (*v1.RawMessage, HandlerError) {
	return &v1.RawMessage{message.Data()}, nil
}

var None = &noneHandler{}
