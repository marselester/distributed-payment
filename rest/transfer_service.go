package rest

import (
	"encoding/json"
	"net/http"
	"strings"

	uuid "github.com/satori/go.uuid"

	wallet "github.com/marselester/distributed-payment"
)

// handlePostTransfer handles requests to create a new money transfer.
// When there is malformed JSON request, 400 status is returned.
func (s *Server) handlePostTransfer() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		var t wallet.Transfer
		if err := json.NewDecoder(r.Body).Decode(&t); err != nil {
			s.handleError(w, errInvalidJSON, http.StatusBadRequest)
			return
		}
		if err := s.validateTransferRequestID(&t); err != nil {
			s.handleError(w, err, http.StatusBadRequest)
			return
		}
		if err := s.validateTransferSender(&t); err != nil {
			s.handleError(w, err, http.StatusBadRequest)
			return
		}
		if err := s.validateTransferAmount(&t); err != nil {
			s.handleError(w, err, http.StatusBadRequest)
			return
		}
		if err := s.validateTransferRecipient(&t); err != nil {
			s.handleError(w, err, http.StatusBadRequest)
			return
		}

		ctx := r.Context()
		switch err := s.transferService.CreateTransfer(ctx, &t); err {
		case nil:
			w.WriteHeader(http.StatusCreated)
			if err = json.NewEncoder(w).Encode(&t); err != nil {
				s.handleError(w, err, http.StatusInternalServerError)
			}
		case wallet.ErrTransferExists:
			s.handleError(w, err, http.StatusBadRequest)
		default:
			s.logger.Log("level", "debug", "msg", "transfer not created", "handler", "PostTransfer", "err", err)
			s.handleError(w, err, http.StatusInternalServerError)
		}
	}
}

// validateTransferRequestID validates whether transfer request ID is UUID.
func (s *Server) validateTransferRequestID(t *wallet.Transfer) error {
	u, err := uuid.FromString(t.ID)
	if err != nil {
		return apiError{
			Message: "transfer request ID must be valid UUID",
			Code:    "request_id_invalid",
		}
	}
	t.ID = u.String()
	return nil
}

// validateTransferSender validates the transfer sender.
func (s *Server) validateTransferSender(t *wallet.Transfer) error {
	t.From = strings.TrimSpace(t.From)
	if t.From == "" {
		return apiError{
			Message: "transfer sender account is required",
			Code:    "from_required",
		}
	}
	return nil
}

// validateTransferAmount validates transfer amount and quantizes it
// according to wallet precision.
func (s *Server) validateTransferAmount(t *wallet.Transfer) error {
	min := s.wopts.minAmount
	if t.Amount.Cmp(&min) == -1 {
		return apiError{
			Message: "ensure this value is greater than " + min.Text('f'),
			Code:    "amount_lt_min",
		}
	}

	// Inexact and Rounded conditions might occur when an amount is quantized.
	// Those conditions are not part of apd.DefaultTraps, so err will be nil.
	if res, err := s.wopts.decimalCtx.Quantize(&t.Amount, &t.Amount, int32(-s.wopts.decimalPlaces)); err != nil {
		return apiError{
			Message: "invalid amount: " + res.String(),
			Code:    "amount_invalid",
		}
	}

	return nil
}

// validateTransferRecipient validates the transfer recipient.
func (s *Server) validateTransferRecipient(t *wallet.Transfer) error {
	t.To = strings.TrimSpace(t.To)
	if t.To == "" {
		return apiError{
			Message: "transfer recipient account is required",
			Code:    "to_required",
		}
	}
	return nil
}

// handleError replies to the request with the specified error and HTTP code.
// Wallet errors include code, generic errors only have a message.
// For example, "problems parsing JSON".
func (s *Server) handleError(w http.ResponseWriter, err error, code int) {
	w.WriteHeader(code)
	if code == http.StatusInternalServerError {
		err = errInternal
	}

	resp, ok := err.(apiError)
	if !ok {
		resp = apiError{
			Message: err.Error(),
		}
	}
	if jsonError := json.NewEncoder(w).Encode(&resp); jsonError != nil {
		s.logger.Log("level", "info", "msg", "err resp: json encode", "jsonError", jsonError, "err", err)
	}
}
