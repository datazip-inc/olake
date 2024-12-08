package protocol

import (
	"fmt"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/datazip-inc/olake/logger"
	"github.com/felixge/fgprof"
	"github.com/gorilla/mux"
)

func init() {
	master := mux.NewRouter()
	master.HandleFunc("/debug/pprof", pprof.Index)
	master.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	master.Handle("/debug/pprof/profile", fgprof.Handler())
	master.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	master.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	master.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	master.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
	master.Handle("/debug/pprof/block", pprof.Handler("block"))

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", 8080),
		Handler:           master,
		ReadTimeout:       time.Second * 60,
		ReadHeaderTimeout: time.Second * 60,
		IdleTimeout:       time.Second * 65,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil {
			logger.Fatal(err)
		}
	}()
}
