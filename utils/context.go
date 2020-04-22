package utils

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

func ContextWithSignal(ctx context.Context) context.Context {
	newCtx, cancel := context.WithCancel(ctx)
	signals := make(chan os.Signal)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case <-signals:
			println("captured signal")
			cancel()
		}
	}()
	return newCtx
}
