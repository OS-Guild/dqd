package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/levigross/grequests"
)

func produce(url string) {
	grequests.Post(url, &grequests.RequestOptions{
		JSON: struct {
			test string
		}{test: "abcd"},
	})
}

func main() {
	target := os.Getenv("HOST")
	if target == "" {
		target = "http://localhost:9990"
	}
	sink := os.Getenv("PRODUCE")
	if sink != "" {
		go produce(fmt.Sprintf("%v/%v", target, sink))
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	select {
	case <-c:
		{

		}
	}

}
