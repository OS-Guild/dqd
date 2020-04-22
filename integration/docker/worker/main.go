package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"
	"github.com/spf13/viper"
)

func main() {
	viper.SetDefault("MESSAGES_COUNT", -1)
	viper.AutomaticEnv()
	ec := viper.GetInt64("MESSAGES_COUNT")
	fmt.Printf("Expecting %v messages", ec)
	router := mux.NewRouter()
	c := int64(0)
	router.Methods("post").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&c, 1)
		body, _ := ioutil.ReadAll(r.Body)
		fmt.Printf("handling message %v: %v", c, string(body))
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(200)
		if c != -1 && c >= ec {
			os.Exit(0)
		}
	})
	println("Listening")
	http.ListenAndServe("0.0.0.0:80", router)
}
