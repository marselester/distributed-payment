// transfer-server is an HTTP server API for clients to create money transfers.
// It exposes REST-style API with basic validation of transfer requests.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/facebookgo/flagenv"
	kitlog "github.com/go-kit/kit/log"

	wallet "github.com/marselester/distributed-payment"
	"github.com/marselester/distributed-payment/kafka"
	"github.com/marselester/distributed-payment/rest"
)

func main() {
	apiAddr := flag.String("http", "127.0.0.1:8000", "HTTP API address.")
	broker := flag.String("broker", "127.0.0.1:9092", "Broker address to connect to.")
	debug := flag.Bool("debug", false, "Enable debug mode.")
	// Parse env values.
	flagenv.Parse()
	// Override env values with command line flag values.
	flag.Parse()

	var logger wallet.Logger
	if *debug {
		w := kitlog.NewSyncWriter(os.Stderr)
		logger = kitlog.NewLogfmtLogger(w)
	} else {
		logger = &wallet.NoopLogger{}
	}

	c := kafka.NewClient(
		kafka.WithBrokers(*broker),
		kafka.WithLogger(logger),
	)
	if err := c.Open(); err != nil {
		log.Fatalf("tranfser-server: failed to connect to Kafka: %v", err)
	}
	defer c.Close()

	api := rest.NewServer(
		rest.WithTransferService(c.Transfer),
		rest.WithLogger(logger),
	)

	// http server could be also placed in rest package to hide net/http dependencies.
	srv := http.Server{
		Addr:         *apiAddr,
		Handler:      api,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  30 * time.Second,
	}
	idleConnsClosed := make(chan struct{})
	go func() {
		sigint := make(chan os.Signal, 1)
		signal.Notify(sigint, os.Interrupt)
		<-sigint

		// We received an interrupt signal, shut down.
		if err := srv.Shutdown(context.Background()); err != nil {
			// Error from closing listeners, or context timeout.
			log.Printf("tranfser-server: api shutdown: %v", err)
		}
		close(idleConnsClosed)
	}()

	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		// Error starting or closing listener.
		log.Printf("tranfser-server: ListenAndServe: %v", err)
	}

	<-idleConnsClosed
	log.Print("tranfser-server: api stopped")
}
