package queue

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"gopkg.in/h2non/gentleman.v2"
	"gopkg.in/h2non/gentleman.v2/plugins/timeout"

	"../metrics"
)

// Handler handles queue messages.
type Handler interface {
	Handle(Message) error
}

type httpHandler struct {
	client *gentleman.Client
}

var handlerLogger = log.With().Str("scope", "Handler")

func combineErrors(err []error) error {
	l := len(err)
	if l == 0 {
		return nil
	}

	errstrings := make([]string, l)
	for i, e := range err {
		errstrings[i] = e.Error()
	}

	return errors.New(strings.Join(errstrings, "\n"))
}

// Handle a queue message.
func (h *httpHandler) Handle(message Message) error {
	end := metrics.StartTimerWithLabels(metrics.HandleMessagesSummary)
	log := handlerLogger.Str("messageId", message.Id()).Logger()
	log.Debug().Msg("Start handling message")

	res, err := h.client.Post().JSON(message.Data()).Send()

	if err == nil && res.ServerError {
		err = fmt.Errorf("Invalid server response: %d", res.StatusCode)
	}
	if err != nil {
		log.Warn().Err(err).Msg("Error handling message")
		end("false")
	} else {
		log.Debug().Msg("Finished handling message")
		end("true")
	}

	return err
}

// NewHandler creates a new http Handler
func NewHandler(endpoint string) Handler {
	client := gentleman.New().
		URL(endpoint).
		Use(timeout.Request(2 * time.Minute))

	return &httpHandler{
		client: client,
	}
}
