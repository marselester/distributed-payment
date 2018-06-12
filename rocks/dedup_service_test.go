package rocks_test

import (
	wallet "github.com/marselester/distributed-payment"
	"github.com/marselester/distributed-payment/rocks"
)

// Ensure rocks.DedupService implements wallet.DedupService interface.
var _ wallet.DedupService = &rocks.DedupService{}
