package tosync

import (
	"context"
)

type Messager interface {
	Pub(context.Context, []byte) (msgID string, err error)
	// DupSub: 需要让pub的msg被所有消费者都消费到，data: msgID -> msgData
	DupSub(context.Context) (data map[string][]byte, err error)
	Ack(ctx context.Context, msgID string) error
}
