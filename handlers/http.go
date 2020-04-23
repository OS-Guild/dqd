package handlers

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	v1 "github.com/soluto/dqd/v1"
	"gopkg.in/h2non/gentleman.v2"
	"gopkg.in/h2non/gentleman.v2/plugins/timeout"
)

type httpHandler struct {
	client *gentleman.Client
}

var handlerLogger = log.With().Str("scope", "Handler")

func (h *httpHandler) Handle(ctx context.Context, message v1.Message) HandlerError {
	res, err := h.client.Post().JSON(message.Data()).Send()

	if err != nil {
		return ServerError(err)
	}
	if err == nil {
		if res.ServerError {
			return ServerError(fmt.Errorf("invalid server response: %d", res.StatusCode))
		}
		if res.ClientError {
			return BadRequestError(fmt.Errorf("invalid client response: %d", res.StatusCode))
		}
	}
	return nil
}

func NewHttpHandler(endpoint string) Handler {
	client := gentleman.New().
		URL(endpoint).
		Use(timeout.Request(2 * time.Minute))

	return &httpHandler{
		client: client,
	}
}
