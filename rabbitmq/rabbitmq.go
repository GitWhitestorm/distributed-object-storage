package rabbitmq

import (
	"encoding/json"

	"github.com/streadway/amqp"
)

type RabbitMq struct {

	// 信道
	channel  *amqp.Channel
	Name     string
	exchange string
}

func New(s string) *RabbitMq {
	conn, err := amqp.Dial(s)
	if err != nil {
		panic(err)
	}

	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}

	queue, err := ch.QueueDeclare(
		"",
		false,
		true,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}
	mq := new(RabbitMq)
	mq.channel = ch
	mq.Name = queue.Name
	return mq
}

func (mq *RabbitMq) Bind(exchange string) {
	err := mq.channel.QueueBind(
		mq.Name,
		"",
		exchange,
		false,
		nil,
	)

	if err != nil {
		panic(err)
	}
	mq.exchange = exchange
}

func (mq *RabbitMq) Send(queue string, body interface{}) {
	str, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}
	err = mq.channel.Publish(
		"",
		queue,
		false,
		false,
		amqp.Publishing{
			Body: []byte(str),
		})
	if err != nil {
		panic(err)
	}
}

func (mq *RabbitMq) Publish(exchange string, body interface{}) {
	str, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}
	err = mq.channel.Publish(
		exchange,
		"",
		false,
		false,
		amqp.Publishing{
			Body: []byte(str),
		})
	if err != nil {
		panic(err)
	}
}

func (mq *RabbitMq) Consume() <-chan amqp.Delivery {
	c, err := mq.channel.Consume(
		mq.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}
	return c
}

func (mq *RabbitMq) Close() {
	mq.channel.Close()
}
