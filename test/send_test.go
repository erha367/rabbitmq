package test

import (
	"context"
	"fmt"
	"github.com/rabbitmq/amqp091-go"
	"log"
	"rabbit/tools"
	"testing"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Printf("%s: %s", msg, err)
	}
}

func TestSendMsg(t *testing.T) {
	conn, err := amqp091.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	//参数：
	//1.queue:队列名称
	//2.durable：是否持久化，当mq重启之后，还在
	//3.exclusive：参数有两个意思 a)是否独占即只能有一个消费者监听这个队列 b)当connection关闭时，是否删除队列
	//4.autoDelete：是否自动删除。当没有Consumer时，自动删除掉
	//5.argument：参数。配置如何删除
	q, err := ch.QueueDeclare(tools.BizQueue, false, false, false, false, nil)
	failOnError(err, "Failed to declare q queue")

	//发送普通消息
	for i := 0; i < 10; i++ {
		msg := fmt.Sprintf("send msg %v", i)
		err = ch.PublishWithContext(context.Background(), "", q.Name, false, false, amqp091.Publishing{
			ContentType:  "test/plain",
			DeliveryMode: amqp091.Persistent,
			Body:         []byte(msg),
		})
		failOnError(err, "Failed to publish a message")
		log.Printf(" [x] Sent %s,%v", msg, i)
	}

	//发送延迟消息
	for i := 0; i < 5; i++ {
		body := fmt.Sprintf("这是一个延迟队列的消息2023:%v", i)
		var exp string
		if i < 2 {
			exp = `10000`
		} else {
			exp = `30000`
		}
		// 将消息发送到延迟队列上
		err = ch.PublishWithContext(
			context.Background(),
			"",               // exchange 这里为空则不选择 exchange
			tools.DelayQueue, // routing key
			false,            // mandatory
			false,            // immediate
			amqp091.Publishing{
				ContentType: "text/plain",
				Body:        []byte(body),
				Expiration:  exp, // 设置10秒的过期时间
			})
		failOnError(err, "Failed to publish a delay message")
	}
}
