package main

import (
	"net/http"
	"os"
	"time"

	"github.com/gorilla/mux"
	"github.com/spf13/viper"
)

func main() {
	viper.SetDefault("MESSAGES_COUNT", -1)
	viper.AutomaticEnv()
	ec := viper.GetInt("MESSAGES_COUNT")
	println(ec)
	router := mux.NewRouter()
	c := 0
	router.Methods("post").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c++
		println("handling message", c)
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(200)
		if c != -1 && c >= ec {
			os.Exit(0)
		}
	})
	println("start listenting")
	http.ListenAndServe("0.0.0.0:80", router)
}
