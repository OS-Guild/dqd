package handlers

import (
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

type HttpHandlerOptions struct {
	Endpoint string
	Method   string
	Host     string
	Headers  map[string]string
}

var handlerLogger = log.With().Str("scope", "Handler")

func (h *httpHandler) Handle(ctx *v1.RequestContext, message v1.Message) (*v1.RawMessage, HandlerError) {
	res, err := h.client.Post().AddHeader("x-dqd-source", ctx.Source()).JSON(message.Data()).Send()
	if err != nil {
		return nil, ServerError(err)
	}
	if err == nil {
		if res.ServerError {
			return nil, ServerError(fmt.Errorf("invalid server response: %d", res.StatusCode))
		}
		if res.ClientError {
			return nil, BadRequestError(fmt.Errorf("invalid client response: %d", res.StatusCode))
		}
	}
	return &v1.RawMessage{
		Data: res.String(),
	}, nil
}

func NewHttpHandler(options *HttpHandlerOptions) Handler {
	client := gentleman.New().
		URL(options.Endpoint).
		Method(options.Method).
		Use(timeout.Request(2 * time.Minute))

	if options.Host != "" {
		client.AddHeader("Host", options.Host)
	}

	if options.Headers != nil {
		for header, value := range options.Headers {
			client.AddHeader(header, value)
		}
	}

	return &httpHandler{
		client,
	}
}
