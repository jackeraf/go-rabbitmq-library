package customRabbitmq

import (
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"

	"github.com/streadway/amqp"
)

// RabbitMqClient .
type RabbitMqClient interface {
	CreateQueue(queue string)
	CreateQueues(queues []string)
	Publish(queue string, message string)
	Consume(queue string)
}

// Rabbitmq .
type Rabbitmq struct {
	channel    *amqp.Channel
	connection *amqp.Connection
}

// NewRabbitmqClient .
func NewRabbitmqClient() RabbitMqClient {
	err := godotenv.Load()
	if err != nil {
		failOnError(err, "Error loading .env file")
	}

	username := os.Getenv("USERNAME")
	if username == "" {
		failOnError(errors.New(""), "Failed to get username env variable")
	}
	password := os.Getenv("PASSWORD")
	if password == "" {
		failOnError(errors.New(""), "Failed to get password env variable")
	}
	url := os.Getenv("URL")
	if url == "" {
		failOnError(errors.New(""), "Failed to get url env variable")
	}
	port := os.Getenv("PORT")
	if port == "" {
		failOnError(errors.New(""), "Failed to get port env variable")
	}

	conn, err := amqp.Dial(fmt.Sprintf("amqp://%v:%v@%v:%v/", username, password, url, port))
	failOnError(err, "Failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	return &Rabbitmq{
		connection: conn,
		channel:    ch,
	}
}

// CreateQueues .
func (rb *Rabbitmq) CreateQueues(queues []string) {
	for _, queue := range queues {
		rb.CreateQueue(queue)
	}
}

// CreateQueue .
func (rb *Rabbitmq) CreateQueue(queue string) {
	_, err := rb.channel.QueueDeclare(
		queue, // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")
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
func (rb *Rabbitmq) Publish(queue string, message string) {
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
	failOnError(err, "Failed to publish a message")
}

// Consume .
func (rb *Rabbitmq) Consume(queue string) {
	msgs, err := rb.channel.Consume(
		queue, // queue
		"",    // consumer
		true,  // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
		}
	}()

	<-forever
}
