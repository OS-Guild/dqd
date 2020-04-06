package handlers

import (
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	v1 "github.com/soluto/dqd/v1"
	"gopkg.in/h2non/gentleman.v2"
	"gopkg.in/h2non/gentleman.v2/plugins/timeout"
)

// Handler handles queue messages.
type Handler interface {
	Handle(v1.Message) error
}

type httpHandler struct {
	client *gentleman.Client
}

var handlerLogger = log.With().Str("scope", "Handler")

func (h *httpHandler) Handle(message v1.Message) error {
	res, err := h.client.Post().JSON(message.Data()).Send()
	if err == nil && res.ServerError {
		err = fmt.Errorf("invalid server response: %d", res.StatusCode)
	}
	return err
}

func NewHttpHandler(endpoint string) Handler {
	client := gentleman.New().
		URL(endpoint).
		Use(timeout.Request(2 * time.Minute))

	return &httpHandler{
		client: client,
	}
}
