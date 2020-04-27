package main

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"
	"github.com/spf13/viper"
)

func main() {
	rand.Seed(10)
	viper.SetDefault("MESSAGES_COUNT", -1)
	viper.AutomaticEnv()
	ec := viper.GetInt64("MESSAGES_COUNT")
	fmt.Printf("Expecting %v messages", ec)
	router := mux.NewRouter()
	n := int64(0)
	t := int64(0)
	m := make(chan bool)
	router.Methods("post").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m <- true
		mNumber := atomic.AddInt64(&n, 1)
		body, _ := ioutil.ReadAll(r.Body)
		fmt.Printf("got %v:%v:%v", mNumber, r.Header.Get("x-dqd-source"), string(body))
		time.Sleep(100 * time.Millisecond)
		sErrRate := r.URL.Query().Get("error")
		if sErrRate != "" {
			errRate, _ := strconv.ParseFloat(sErrRate, 64)
			if rand.Float64() < errRate {
				w.WriteHeader(500)
				return
			}
		}
		w.WriteHeader(200)
		p := atomic.AddInt64(&t, 1)
		fmt.Printf("handled %v message\n", p)
		if p != -1 && p == ec {
			time.AfterFunc(1*time.Second, func() { os.Exit(0) })
		}
	})
	println("Listening")
	go func() {
		<-m
		for {
			select {
			case <-m:
			case <-time.After(10 * time.Second):
				os.Exit(1)
			}
		}

	}()
	http.ListenAndServe("0.0.0.0:80", router)
}
