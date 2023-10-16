package main

import (
	"bytes"
	"github.com/rabbitmq/amqp091-go"
	"log"
	"rabbit/tools"
	"time"
)

func main() {
	conn, err := amqp091.Dial("amqp://guest:guest@localhost:5672/")
	tools.FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	tools.FailOnError(err, "Failed to open a channel")
	defer ch.Close()

	/*- 工作队列 -*/

	q, err := ch.QueueDeclare(tools.BizQueue, false, false, false, false, nil)
	tools.FailOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	tools.FailOnError(err, "Failed to register a consumer")

	var forever chan struct{}
	//消费工作队列
	go func() {
		for d := range msgs {
			log.Printf("工作队列 [x]: %s", d.Body)
			dotCount := bytes.Count(d.Body, []byte("."))
			t := time.Duration(dotCount)
			time.Sleep(t * time.Second)
			log.Printf("Done")
		}
	}()

	/*- 延迟队列 -*/
	// 声明一个主要使用的 exchange
	err = ch.ExchangeDeclare(
		tools.DelayExchange, // name
		"fanout",            // type广播形式，绑定的都会推送消息
		false,               // durable
		false,               // auto-deleted
		false,               // internal
		false,               // no-wait
		nil,                 // arguments
	)
	tools.FailOnError(err, "Failed to declare an exchange")

	//声明一个普通队列，来处理死信消息
	_, err = ch.QueueDeclare(tools.DelayHandlerQueue, false, false, false, false, nil)
	tools.FailOnError(err, "Failed to declare a queue")

	/**
	 * 注意,这里是重点!!!!!
	 * 声明一个延时队列, ß我们的延时消息就是要发送到这里
	 */
	_, err = ch.QueueDeclare(
		tools.DelayQueue, // name
		false,            // durable
		false,            // delete when unused
		false,            // exclusive
		false,            // no-wait
		amqp091.Table{
			// 当消息过期时把消息发送到 logs 这个 exchange
			"x-dead-letter-exchange": tools.DelayExchange,
		}, // arguments
	)
	tools.FailOnError(err, "fail to declare a queue")

	err = ch.QueueBind(
		tools.DelayHandlerQueue, // queue name, 这里指的是 test_logs
		"",                      // routing key
		tools.DelayExchange,     // exchange
		false,
		nil,
	)
	tools.FailOnError(err, "Failed to bind a queue")

	// 这里监听的是 test_logs
	msg2, err := ch.Consume(
		tools.DelayHandlerQueue, // queue name, 这里指的是 test_logs
		"",                      // consumer
		true,                    // auto-ack
		false,                   // exclusive
		false,                   // no-local
		false,                   // no-wait
		nil,                     // args
	)
	tools.FailOnError(err, "Failed to consume a queue")

	go func() {
		for d := range msg2 {
			log.Printf("接收延迟消息 [y] %s", d.Body)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
