// Package kafka implements wallet services and provides the Client to access them.
// There were two design options: group the services in the Client or
// inject a service into each other. For example, kafka.TransferService.PaymentService = pg.NewPaymentService()
// lets have a swappable PaymentService (PostgreSQL implementation in the example).
// Grouping services in the Client allows them to maintain DB transactions by sharing
// the same *sql.DB in the Client.
// In practise you'll likely have one or two storages. Swapping implementations during a lifetime of
// the project is almost non existent.
package kafka

import (
	"github.com/Shopify/sarama"

	wallet "github.com/marselester/distributed-payment"
)

const (
	// DefaultTransferTopic is a default topic where transfer requests are sent.
	DefaultTransferTopic = "wallet.transfer_request"
	// DefaultPaymentTopic is a default topic where payments are emitted.
	DefaultPaymentTopic = "wallet.payment"
)

// Client represents a client to the underlying Kafka commit log.
type Client struct {
	Transfer wallet.TransferService
	Payment  wallet.PaymentService

	logger   wallet.Logger
	consumer sarama.Consumer
	producer sarama.SyncProducer

	copts connOption
}

// connOption holds connection settings.
type connOption struct {
	brokers       []string
	transferTopic string
	paymentTopic  string
}

// NewClient returns a new Client which provides you with
// transfer and payment services based on Kafka.
func NewClient(options ...ConfigOption) *Client {
	c := Client{
		logger: &wallet.NoopLogger{},
		copts: connOption{
			transferTopic: DefaultTransferTopic,
			paymentTopic:  DefaultPaymentTopic,
		},
	}
	c.Transfer = &TransferService{client: &c}
	c.Payment = &PaymentService{client: &c}

	for _, opt := range options {
		opt(&c)
	}
	return &c
}

// ConfigOption configures the Client.
type ConfigOption func(*Client)

// WithLogger configures a logger to debug interactions with Kafka.
func WithLogger(l wallet.Logger) ConfigOption {
	return func(c *Client) {
		c.logger = l
	}
}

// WithBrokers sets brokers to connect to, for example, []string{"127.0.0.1:9092"}.
func WithBrokers(brokers ...string) ConfigOption {
	return func(c *Client) {
		c.copts.brokers = brokers
	}
}

// Open creates Kafka consumer and producer.
// Make sure you call Close to clean up resources.
func (c *Client) Open() error {
	var err error
	c.consumer, err = sarama.NewConsumer(c.copts.brokers, nil)
	if err != nil {
		c.logger.Log("level", "debug", "msg", "consumer not created", "err", err)
		return err
	}
	c.logger.Log("level", "debug", "msg", "consumer created")

	c.producer, err = sarama.NewSyncProducer(c.copts.brokers, nil)
	if err != nil {
		c.logger.Log("level", "debug", "msg", "producer not created", "err", err)
		return err
	}
	c.logger.Log("level", "debug", "msg", "producer created")
	return nil
}

// Close shuts down the producer and waits for any buffered messages to be flushed.
// It also closes the consumer.
func (c *Client) Close() {
	c.consumer.Close()
	c.logger.Log("level", "debug", "msg", "consumer closed")

	c.producer.Close()
	c.logger.Log("level", "debug", "msg", "producer closed")
}
