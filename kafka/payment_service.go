package kafka

import (
	"context"
	"encoding/json"

	"github.com/Shopify/sarama"

	wallet "github.com/marselester/distributed-payment"
)

// PaymentService represents a Kafka service to store payment instructions.
type PaymentService struct {
	client *Client
}

// CreatePayment persists a payment encoded as JSON message.
func (s *PaymentService) CreatePayment(ctx context.Context, p *wallet.Payment) error {
	b, err := json.Marshal(p)
	if err != nil {
		return err
	}
	s.client.logger.Log("level", "debug", "msg", "creating payment", "body", b)

	m := sarama.ProducerMessage{
		Topic: s.client.copts.paymentTopic,
		// Sarama uses the message's key to consistently assign a partition to a message using hashing.
		Key:   sarama.StringEncoder(p.Account),
		Value: sarama.ByteEncoder(b),
	}
	partition, offset, err := s.client.producer.SendMessage(&m)
	if err != nil {
		s.client.logger.Log("level", "debug", "msg", "payment not created", "topic", s.client.copts.paymentTopic, "body", b, "err", err)
		return err
	}
	p.Partition = partition
	p.SequenceID = offset

	s.client.logger.Log("level", "debug", "msg", "payment created", "partition", partition, "offset", offset, "body", b)
	return nil
}

// FromOffset returns a channel of payments from the given partition starting at offset.
func (s *PaymentService) FromOffset(ctx context.Context, partition int32, offset int64) (<-chan *wallet.Payment, <-chan error) {
	payments := make(chan *wallet.Payment)
	errc := make(chan error, 1)

	go func() {
		// Close the payments channel after messages returns.
		defer close(payments)

		err := s.messages(ctx, s.client.copts.paymentTopic, partition, offset, func(m *sarama.ConsumerMessage) error {
			p := wallet.Payment{}
			if err := json.Unmarshal(m.Value, &p); err != nil {
				return err
			}
			p.Partition = m.Partition
			p.SequenceID = m.Offset

			select {
			case payments <- &p:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		})

		// No select needed for this send, since errc is buffered.
		errc <- err
	}()

	return payments, errc
}

// messages fetches Kafka messages from the given partition and calls f.
func (s *PaymentService) messages(ctx context.Context, topic string, partition int32, offset int64, f func(*sarama.ConsumerMessage) error) error {
	s.client.logger.Log("level", "debug", "msg", "messages reading started", "topic", topic, "partition", partition, "offset", offset)

	pConsumer, err := s.client.consumer.ConsumePartition(topic, partition, offset)
	if err != nil {
		s.client.logger.Log("level", "debug", "msg", "messages consumer not created", "err", err, "topic", topic, "partition", partition, "offset", offset)
		return err
	}
	// Terminate message consuming by closing Messages channel when ctx is cancelled.
	go func() {
		<-ctx.Done()
		pConsumer.Close()
		s.client.logger.Log("level", "debug", "msg", "messages consumer closed", "err", ctx.Err(), "topic", topic, "partition", partition, "offset", offset)
	}()

	for m := range pConsumer.Messages() {
		s.client.logger.Log("level", "debug", "msg", "message received", "body", m.Value, "topic", topic, "partition", partition, "offset", offset)
		f(m)
	}

	s.client.logger.Log("level", "debug", "msg", "messages reading stopped", "topic", topic, "partition", partition, "offset", offset)
	return nil
}
