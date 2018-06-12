package kafka

import (
	"context"
	"encoding/json"

	"github.com/Shopify/sarama"

	wallet "github.com/marselester/distributed-payment"
)

// TransferService represents a Kafka service to store money transfer requests.
type TransferService struct {
	client *Client
}

// CreateTransfer persists money transfer request encoded as JSON message.
func (s *TransferService) CreateTransfer(ctx context.Context, t *wallet.Transfer) error {
	b, err := json.Marshal(t)
	if err != nil {
		return err
	}
	s.client.logger.Log("level", "debug", "msg", "creating transfer", "body", b)

	m := sarama.ProducerMessage{
		Topic: s.client.copts.transferTopic,
		// Sarama uses the message's key to consistently assign a partition to a message using hashing.
		Key:   sarama.StringEncoder(t.ID),
		Value: sarama.ByteEncoder(b),
	}
	partition, offset, err := s.client.producer.SendMessage(&m)
	if err != nil {
		s.client.logger.Log("level", "debug", "msg", "transfer not created", "topic", s.client.copts.transferTopic, "body", b, "err", err)
		return err
	}

	s.client.logger.Log("level", "debug", "msg", "transfer created", "partition", partition, "offset", offset, "body", b)
	return nil
}

// FromOffset returns a channel of transfers from the given partition starting at offset.
func (s *TransferService) FromOffset(ctx context.Context, partition int32, offset int64) (<-chan *wallet.Transfer, <-chan error) {
	transfers := make(chan *wallet.Transfer)
	errc := make(chan error, 1)

	go func() {
		// Close the transfers channel after messages returns.
		defer close(transfers)

		err := s.messages(ctx, s.client.copts.transferTopic, partition, offset, func(m *sarama.ConsumerMessage) error {
			t := wallet.Transfer{}
			if err := json.Unmarshal(m.Value, &t); err != nil {
				return err
			}
			t.Partition = m.Partition
			t.SequenceID = m.Offset

			select {
			case transfers <- &t:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		})

		// No select needed for this send, since errc is buffered.
		errc <- err
	}()

	return transfers, errc
}

// messages fetches Kafka messages from the given partition and calls f.
func (s *TransferService) messages(ctx context.Context, topic string, partition int32, offset int64, f func(*sarama.ConsumerMessage) error) error {
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
