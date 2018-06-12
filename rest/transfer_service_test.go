package rest_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	wallet "github.com/marselester/distributed-payment"
	"github.com/marselester/distributed-payment/mock"
	"github.com/marselester/distributed-payment/rest"
)

func TestTransferService_CreateTransfer_ErrInvalidJSON(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		body       string
		want       string
	}{
		{
			name:       "body=",
			statusCode: http.StatusBadRequest,
			body:       "",
			want:       `{"message":"problems parsing JSON"}` + "\n",
		},
		{
			name:       "body={",
			statusCode: http.StatusBadRequest,
			body:       "{",
			want:       `{"message":"problems parsing JSON"}` + "\n",
		},
		{
			name:       "body=]",
			statusCode: http.StatusBadRequest,
			body:       "]",
			want:       `{"message":"problems parsing JSON"}` + "\n",
		},
		{
			name:       "amount=int",
			statusCode: http.StatusBadRequest,
			body:       `{"amount": 1}`,
			want:       `{"message":"problems parsing JSON"}` + "\n",
		},
	}

	srv := rest.NewServer(
		rest.WithTransferService(&mock.TransferService{}),
	)

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := httptest.NewRequest("POST", "/api/v1/transfers", strings.NewReader(tc.body))
			w := httptest.NewRecorder()
			srv.ServeHTTP(w, r)

			body := w.Body.String()
			if body != tc.want {
				t.Fatalf("body: %s, want %s", body, tc.want)
			}

			resp := w.Result()
			if resp.StatusCode != tc.statusCode {
				t.Fatalf("status code: %d, want %d", resp.StatusCode, tc.statusCode)
			}

			ct := resp.Header.Get("Content-Type")
			if ct != "application/json" {
				t.Fatalf("content type: %q, want application/json", ct)
			}
		})
	}
}

func TestTransferService_CreateTransfer_ErrInternal(t *testing.T) {
	m := mock.TransferService{
		CreateTransferFn: func(_ context.Context, t *wallet.Transfer) error {
			return errors.New("unknown error")
		},
	}
	srv := rest.NewServer(
		rest.WithTransferService(&m),
	)

	r := httptest.NewRequest("POST", "/api/v1/transfers", strings.NewReader(`{
		"request_id": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
		"from": "Alice",
		"amount": "1",
		"to": "Bob"
	}`))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	body := w.Body.String()
	want := `{"message":"internal error"}` + "\n"
	if body != want {
		t.Fatalf("body: %s, want %s", body, want)
	}

	resp := w.Result()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status code: %d, want %d", resp.StatusCode, http.StatusInternalServerError)
	}

	ct := resp.Header.Get("Content-Type")
	if ct != "application/json" {
		t.Fatalf("content type: %q, want application/json", ct)
	}
}

func TestTransferService_CreateTransfer_RequestIDValidation(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		body       string
		want       string
	}{
		{
			name:       "std uuid form",
			statusCode: http.StatusCreated,
			body: `{
				"request_id": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
				"from": "Alice",
				"amount": "1",
				"to": "Bob"
			}`,
			want: `"request_id":"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"`,
		},
		{
			name:       "upper case",
			statusCode: http.StatusCreated,
			body: `{
				"request_id": "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11",
				"from": "Alice",
				"amount": "1",
				"to": "Bob"
			}`,
			want: `"request_id":"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"`,
		},
		{
			name:       "no hyphens",
			statusCode: http.StatusCreated,
			body: `{
				"request_id": "a0eebc999c0b4ef8bb6d6bb9bd380a11",
				"from": "Alice",
				"amount": "1",
				"to": "Bob"
			}`,
			want: `"request_id":"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"`,
		},
		{
			name:       "empty",
			statusCode: http.StatusBadRequest,
			body: `{
				"request_id": "",
				"from": "Alice",
				"amount": "1",
				"to": "Bob"
			}`,
			want: `{"message":"transfer request ID must be valid UUID","code":"request_id_invalid"}`,
		},
	}

	srv := rest.NewServer(
		rest.WithTransferService(&mock.TransferService{}),
	)

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := httptest.NewRequest("POST", "/api/v1/transfers", strings.NewReader(tc.body))
			w := httptest.NewRecorder()
			srv.ServeHTTP(w, r)

			body := w.Body.String()
			if !strings.Contains(body, tc.want) {
				t.Fatalf("body: %s, want %s", body, tc.want)
			}

			resp := w.Result()
			if resp.StatusCode != tc.statusCode {
				t.Fatalf("status code: %d, want %d", resp.StatusCode, tc.statusCode)
			}

			ct := resp.Header.Get("Content-Type")
			if ct != "application/json" {
				t.Fatalf("content type: %q, want application/json", ct)
			}
		})
	}
}

func TestTransferService_CreateTransfer_SenderValidation(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		body       string
		want       string
	}{
		{
			name:       "missing from",
			statusCode: http.StatusBadRequest,
			body: `{
				"request_id": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
				"amount": "1",
				"to": "Bob"
			}`,
			want: `{"message":"transfer sender account is required","code":"from_required"}`,
		},
		{
			name:       "from is cleaned",
			statusCode: http.StatusCreated,
			body: `{
				"request_id": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
				"from": " \t Alice \n ",
				"amount": "1",
				"to": "Bob"
			}`,
			want: `"from":"Alice"`,
		},
	}

	srv := rest.NewServer(
		rest.WithTransferService(&mock.TransferService{}),
	)

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := httptest.NewRequest("POST", "/api/v1/transfers", strings.NewReader(tc.body))
			w := httptest.NewRecorder()
			srv.ServeHTTP(w, r)

			body := w.Body.String()
			if !strings.Contains(body, tc.want) {
				t.Fatalf("body: %s, want %s", body, tc.want)
			}

			resp := w.Result()
			if resp.StatusCode != tc.statusCode {
				t.Fatalf("status code: %d, want %d", resp.StatusCode, tc.statusCode)
			}

			ct := resp.Header.Get("Content-Type")
			if ct != "application/json" {
				t.Fatalf("content type: %q, want application/json", ct)
			}
		})
	}
}

func TestTransferService_CreateTransfer_AmountValidation(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		body       string
		want       string
	}{
		{
			name:       "amount lt 0",
			statusCode: http.StatusBadRequest,
			body: `{
				"request_id": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
				"from": "Alice",
				"amount": "-0.01",
				"to": "Bob"
			}`,
			want: `{"message":"ensure this value is greater than 0.01","code":"amount_lt_min"}`,
		},
		{
			name:       "amount eq 0",
			statusCode: http.StatusBadRequest,
			body: `{
				"request_id": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
				"from": "Alice",
				"amount": "0",
				"to": "Bob"
			}`,
			want: `{"message":"ensure this value is greater than 0.01","code":"amount_lt_min"}`,
		},
		{
			name:       "amount lt min",
			statusCode: http.StatusBadRequest,
			body: `{
				"request_id": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
				"from": "Alice",
				"amount": "0.009",
				"to": "Bob"
			}`,
			want: `{"message":"ensure this value is greater than 0.01","code":"amount_lt_min"}`,
		},
		{
			name:       "amount eq 0.000000000000000000000000000001",
			statusCode: http.StatusBadRequest,
			body: `{
				"request_id": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
				"from": "Alice",
				"amount": "0.000000000000000000000000000001",
				"to": "Bob"
			}`,
			want: `{"message":"ensure this value is greater than 0.01","code":"amount_lt_min"}`,
		},
		{
			name:       "amount eq 1e-30",
			statusCode: http.StatusBadRequest,
			body: `{
				"request_id": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
				"from": "Alice",
				"amount": "1e-30",
				"to": "Bob"
			}`,
			want: `{"message":"ensure this value is greater than 0.01","code":"amount_lt_min"}`,
		},
		{
			name:       "amount 1.009 quantized up",
			statusCode: http.StatusCreated,
			body: `{
				"request_id": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
				"from": "Alice",
				"amount": "1.009",
				"to": "Bob"
			}`,
			want: `"amount":"1.01"`,
		},
		{
			name:       "amount 1.001 quantized down",
			statusCode: http.StatusCreated,
			body: `{
				"request_id": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
				"from": "Alice",
				"amount": "1.001",
				"to": "Bob"
			}`,
			want: `"amount":"1.00"`,
		},
		{
			name:       "amount eq min",
			statusCode: http.StatusCreated,
			body: `{
				"request_id": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
				"from": "Alice",
				"amount": "0.01",
				"to": "Bob"
			}`,
			want: `"amount":"0.01"`,
		},
	}

	srv := rest.NewServer(
		rest.WithTransferService(&mock.TransferService{}),
	)

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := httptest.NewRequest("POST", "/api/v1/transfers", strings.NewReader(tc.body))
			w := httptest.NewRecorder()
			srv.ServeHTTP(w, r)

			body := w.Body.String()
			if !strings.Contains(body, tc.want) {
				t.Fatalf("body: %s, want %s", body, tc.want)
			}

			resp := w.Result()
			if resp.StatusCode != tc.statusCode {
				t.Fatalf("status code: %d, want %d", resp.StatusCode, tc.statusCode)
			}

			ct := resp.Header.Get("Content-Type")
			if ct != "application/json" {
				t.Fatalf("content type: %q, want application/json", ct)
			}
		})
	}
}

func TestTransferService_CreateTransfer_RecipientValidation(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		body       string
		want       string
	}{
		{
			name:       "missing to",
			statusCode: http.StatusBadRequest,
			body: `{
				"request_id": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
				"from": "Alice",
				"amount": "1"
			}`,
			want: `{"message":"transfer recipient account is required","code":"to_required"}`,
		},
		{
			name:       "to is cleaned",
			statusCode: http.StatusCreated,
			body: `{
				"request_id": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
				"from": "Alice",
				"amount": "1",
				"to": " \t Bob \n "
			}`,
			want: `"to":"Bob"`,
		},
	}

	srv := rest.NewServer(
		rest.WithTransferService(&mock.TransferService{}),
	)

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := httptest.NewRequest("POST", "/api/v1/transfers", strings.NewReader(tc.body))
			w := httptest.NewRecorder()
			srv.ServeHTTP(w, r)

			body := w.Body.String()
			if !strings.Contains(body, tc.want) {
				t.Fatalf("body: %s, want %s", body, tc.want)
			}

			resp := w.Result()
			if resp.StatusCode != tc.statusCode {
				t.Fatalf("status code: %d, want %d", resp.StatusCode, tc.statusCode)
			}

			ct := resp.Header.Get("Content-Type")
			if ct != "application/json" {
				t.Fatalf("content type: %q, want application/json", ct)
			}
		})
	}
}
