package tools

import "fmt"

const (
	BizQueue          = `ybb_biz_queue`     //业务队列
	DelayQueue        = `ybb_delay_queue`   //延迟队列（死信）
	DelayHandlerQueue = `ybb_handler_queue` //延迟处理队列

	DelayExchange = `ybb_delay_exchange` //交换机
)

func FailOnError(err error, msg string) {
	if err != nil {
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}
