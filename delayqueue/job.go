package delayqueue

import (
	"errors"
	"fmt"
	"time"
)

type Job struct {
	Id       int    `json:"id"`       // Job唯一标识ID，确保唯一
	Topic    string `json:"topic"`    // 真正投递的消费队列
	Body     string `json:"body"`     // Job消息体
	Delay    int64  `json:"delay"`    // Job需要延迟的时间, 单位：秒
	ExecTime int64  `json:"execTime"` // Job执行的时间, 单位：秒
}

func (j *Job) Validate() error {
	if j.Topic == "" || j.Delay < 0 {
		return fmt.Errorf("invalid job %+v", j)
	}
	if j.ExecTime < time.Now().Unix() {
		return errors.New("job already expire before send to queue")
	}
	return nil
}
