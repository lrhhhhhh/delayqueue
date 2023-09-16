package job

import (
	"errors"
	"fmt"
	"time"
)

type Job struct {
	Id         int    `json:"id"`       // Job唯一标识ID，确保唯一
	Topic      string `json:"topic"`    // 真正投递的消费队列
	Body       string `json:"body"`     // Job消息体
	DelayMs    int64  `json:"delay"`    // Job需要延迟的时间, 单位：毫秒
	ExecTimeMs int64  `json:"execTime"` // Job执行的时间, 单位：毫秒
}

func (j *Job) Validate() error {
	if j.Topic == "" || j.DelayMs < 0 {
		return fmt.Errorf("invalid job %+v", j)
	}
	if j.ExecTimeMs < time.Now().UnixMilli() {
		return errors.New("job already expire before send to queue")
	}
	return nil
}
