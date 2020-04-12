package customRabbitmq

import (
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/streadway/amqp"
)

// RabbitMqClient .
type RabbitMqClient interface {
	CreateQueue(queue string) error
	CreateQueues(queues []string) error
	Publish(queue string, message string) error
	Consume(queue string) error
}

// Rabbitmq .
type Rabbitmq struct {
	channel    *amqp.Channel
	connection *amqp.Connection
}

// NewRabbitmqClient .
func NewRabbitmqClient() (RabbitMqClient, error) {
	username := os.Getenv("USER")
	if username == "" {
		return nil, errors.New("Failed to get username env variable")
	}
	password := os.Getenv("PASSWORD")
	if password == "" {
		return nil, errors.New("Failed to get password env variable")
	}
	url := os.Getenv("URL")
	if url == "" {
		return nil, errors.New("Failed to get url env variable")
	}
	port := os.Getenv("PORT")
	if port == "" {
		return nil, errors.New("Failed to get port env variable")
	}
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%v:%v@%v:%v/", username, password, url, port))
	if err != nil {
		return nil, fmt.Errorf("Failed to connect to RabbitMQ amqp://%v:%v@%v:%v/", username, password, url, port)
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, errors.New("Failed to open a channel")
	}
	return &Rabbitmq{
		connection: conn,
		channel:    ch,
	}, nil
}

// CreateQueues .
func (rb *Rabbitmq) CreateQueues(queues []string) error {
	for _, queue := range queues {
		err := rb.CreateQueue(queue)
		if err != nil {
			return err
		}
	}
	return nil
}

// CreateQueue .
func (rb *Rabbitmq) CreateQueue(queue string) error {
	_, err := rb.channel.QueueDeclare(
		queue, // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return errors.New("Failed to declare a queue")
	}
	return nil
}

// CloseConnection .
func (rb *Rabbitmq) CloseConnection() {
	rb.connection.Close()
}

// CloseChannel .
func (rb *Rabbitmq) CloseChannel() {
	rb.channel.Close()
}

// Publish .
func (rb *Rabbitmq) Publish(queue string, message string) error {
	err := rb.channel.Publish(
		"",    // exchange
		queue, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
	log.Printf(" [x] Sent %s", message)
	if err != nil {
		return errors.New("Failed to publish a message")
	}
	return nil
}

// Consume .
func (rb *Rabbitmq) Consume(queue string) error {
	msgs, err := rb.channel.Consume(
		queue, // queue
		"",    // consumer
		true,  // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return errors.New("Failed to register a consumer")
	}

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
		}
	}()

	<-forever
	return nil
}
