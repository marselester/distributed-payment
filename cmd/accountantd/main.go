// Command accountantd sequentially reads Kafka messages from wallet.payment topic,
// deduplicates messages by request ID, and applies the changes to the account balances.
// You can replay Kafka messages from any offset, as long as request IDs are persisted.
// If the program crashes, it should recover dedup db based on Kafka topic ("source of truth").
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/cockroachdb/apd"
	"github.com/facebookgo/flagenv"
	kitlog "github.com/go-kit/kit/log"
	"github.com/pkg/errors"

	wallet "github.com/marselester/distributed-payment"
	"github.com/marselester/distributed-payment/kafka"
	"github.com/marselester/distributed-payment/rocks"
)

func main() {
	broker := flag.String("broker", "127.0.0.1:9092", "Broker address to connect to.")
	partition := flag.Int("partition", 0, "Partition number of wallet.payment topic.")
	offset := flag.Int64("offset", -2, "Offset index of a partition (-1 to start from the newest, -2 from the oldest).")
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
		log.Fatalf("accountantd: failed to connect to Kafka: %v", err)
	}
	defer c.Close()

	dbname := fmt.Sprintf("dedup%d.db", *partition)
	dedup := rocks.NewDedupService(
		rocks.WithDB(dbname),
		rocks.WithLogger(logger),
	)
	if err := dedup.Open(); err != nil {
		log.Fatalf("accountantd: failed to open RocksDB: %v", err)
	}
	defer dedup.Close()

	// Listen to Ctrl+C and kill/killall to gracefully stop processing payments.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt)
	go func() {
		<-sigchan
		cancel()
	}()

	balance := make(map[string]apd.Decimal)
	payments, errc := c.Payment.FromOffset(ctx, int32(*partition), *offset)
	for p := range payments {
		// Deduplicate based on request ID, then update balance.
		seen, err := dedup.HasSeen(p.RequestID)
		if err != nil {
			log.Fatalf("accountantd: failed to read from RocksDB: %v", err)
		}
		if seen {
			logger.Log("level", "debug", "msg", "skip request", "request", p.RequestID)
			continue
		}

		bal, err := calcBalance(balance[p.Account], p)
		if err != nil {
			log.Fatalf("accountantd: failed to update balance: %v", err)
		}
		balance[p.Account] = bal
		fmt.Printf("%s $%s\n", p.Account, bal.Text('f'))

		if err := dedup.Save(p.RequestID); err != nil {
			log.Fatalf("accountantd: failed to save request ID: %v", err)
		}
	}
	if err := <-errc; err != nil {
		log.Printf("accountantd: payments fetch failed: %v", err)
	}
}

// calcBalance calculates account balance affected by a payment.
func calcBalance(bal apd.Decimal, p *wallet.Payment) (apd.Decimal, error) {
	dc := apd.BaseContext.WithPrecision(wallet.DecimalMaxDigits)

	if p.Direction == "outgoing" {
		if res, err := dc.Sub(&bal, &bal, &p.Amount); err != nil {
			return bal, errors.Wrapf(err, "incoming payment: %v", res)
		}
		return bal, nil
	}

	if res, err := dc.Add(&bal, &bal, &p.Amount); err != nil {
		return bal, errors.Wrapf(err, "outgoing payment: %v", res)
	}
	return bal, nil
}
