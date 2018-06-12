// Package rest is responsible for translating incoming requests to wallet domain
// and then translate results from wallet model back to HTTP responses.
package rest

import (
	"net/http"

	"github.com/cockroachdb/apd"
	"github.com/go-chi/chi"

	wallet "github.com/marselester/distributed-payment"
)

// Server represents an HTTP API handler for wallet services. It wraps a TransferService
// so we can provide different implementations, e.g., Kafka or a mock.
type Server struct {
	*chi.Mux
	logger          wallet.Logger
	transferService wallet.TransferService
	wopts           walletOption
}

// walletOption configures wallet API server.
type walletOption struct {
	maxDigits     uint32
	decimalPlaces uint32
	decimalCtx    *apd.Context

	minAmount apd.Decimal
}

// NewServer registers HTTP handlers and returns a new server.
func NewServer(options ...ConfigOption) *Server {
	srv := Server{
		Mux:    chi.NewRouter(),
		logger: &wallet.NoopLogger{},
		wopts: walletOption{
			maxDigits:     wallet.DecimalMaxDigits,
			decimalPlaces: wallet.DecimalPlaces,
			decimalCtx:    apd.BaseContext.WithPrecision(wallet.DecimalMaxDigits),
		},
	}
	min, _, _ := apd.NewFromString("0.01")
	srv.wopts.minAmount = *min

	for _, opt := range options {
		opt(&srv)
	}

	srv.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status": "ok"}`))
	})
	srv.Post("/api/v1/transfers", srv.handlePostTransfer())

	return &srv
}

// ConfigOption configures the API server.
type ConfigOption func(*Server)

// WithLogger configures a structured logger.
func WithLogger(l wallet.Logger) ConfigOption {
	return func(srv *Server) {
		srv.logger = l
	}
}

// WithTransferService configures server to use a transfer service.
func WithTransferService(s wallet.TransferService) ConfigOption {
	return func(srv *Server) {
		srv.transferService = s
	}
}

// WithPrecision lets you set the decimal precision.
func WithPrecision(maxDigits, decimalPlaces uint32) ConfigOption {
	return func(srv *Server) {
		srv.wopts.maxDigits = maxDigits
		srv.wopts.decimalPlaces = decimalPlaces
		srv.wopts.decimalCtx = apd.BaseContext.WithPrecision(maxDigits)
	}
}
