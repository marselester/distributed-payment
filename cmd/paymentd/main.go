// Command paymentd is responsible to create incoming & outgoing payment pair based on money transfer request.
// You can replay Kafka messages from any offset, duplicates are skipped by the next process in the pipeline.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/facebookgo/flagenv"
	kitlog "github.com/go-kit/kit/log"

	wallet "github.com/marselester/distributed-payment"
	"github.com/marselester/distributed-payment/kafka"
)

func main() {
	broker := flag.String("broker", "127.0.0.1:9092", "Broker address to connect to.")
	partition := flag.Int("partition", 0, "Partition number of wallet.transfer_request topic.")
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
		log.Fatalf("paymentd: failed to connect to Kafka: %v", err)
	}
	defer c.Close()

	// Listen to Ctrl+C and kill/killall to gracefully stop processing transfer requests.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt)
	go func() {
		<-sigchan
		cancel()
	}()

	transfers, errc := c.Transfer.FromOffset(ctx, int32(*partition), *offset)
	for t := range transfers {
		outPay := wallet.Payment{
			RequestID: t.ID,
			Account:   t.From,
			Direction: "outgoing",
			Amount:    t.Amount,
		}
		if err := c.Payment.CreatePayment(ctx, &outPay); err != nil {
			log.Fatalf("paymentd: create outgoing payment: %v", err)
		}
		fmt.Printf("%d:%d %s %s -$%s\n", outPay.Partition, outPay.SequenceID, outPay.RequestID, outPay.Account, outPay.Amount.Text('f'))

		inPay := wallet.Payment{
			RequestID: t.ID,
			Account:   t.To,
			Direction: "incoming",
			Amount:    t.Amount,
		}
		if err := c.Payment.CreatePayment(ctx, &inPay); err != nil {
			log.Fatalf("paymentd: create incoming payment: %v", err)
		}
		fmt.Printf("%d:%d %s %s +$%s\n", inPay.Partition, inPay.SequenceID, inPay.RequestID, inPay.Account, inPay.Amount.Text('f'))
	}
	if err := <-errc; err != nil {
		log.Printf("paymentd: transfers fetch failed: %v", err)
	}
}
