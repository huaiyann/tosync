package messager

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logc"
)

var (
	MsgRetainDur = time.Minute * 10
	ReadBlockDur = time.Second * 1
)

type MsgID struct {
	MsTimestamp int64
	Seq         int64
}

func (m *MsgID) String() string {
	return fmt.Sprintf("%d-%d", m.MsTimestamp, m.Seq)
}

func (m *MsgID) Gt(v *MsgID) bool {
	if m.MsTimestamp == v.MsTimestamp {
		return m.Seq > v.Seq
	}
	return m.MsTimestamp > v.MsTimestamp
}

func (m *MsgID) Add(t time.Duration) *MsgID {
	newTs := m.MsTimestamp + t.Milliseconds()
	if newTs <= 0 {
		return new(MsgID)
	}
	return &MsgID{
		MsTimestamp: newTs,
		Seq:         m.Seq,
	}
}

func ParseMsgID(s string) (*MsgID, error) {
	var msTimestamp, seq int64
	_, err := fmt.Sscanf(s, "%d-%d", &msTimestamp, &seq)
	if err != nil {
		return nil, err
	}
	return &MsgID{
		MsTimestamp: msTimestamp,
		Seq:         seq,
	}, nil
}

type RedisMessager struct {
	lock      *sync.RWMutex
	cli       *redis.Client
	stream    string
	lastPubID *MsgID
	lastSubID *MsgID
}

func NewRedisMessager(cli *redis.Client, stream string) (*RedisMessager, error) {
	// 每次从最新开始消费，用redis server的时间戳（减一个dur）作为初始水位
	t, err := cli.Time(context.Background()).Result()
	if err != nil {
		return nil, errors.Wrap(err, "get redis time")
	}
	t = t.Add(-time.Second)
	return &RedisMessager{
		lock:   &sync.RWMutex{},
		cli:    cli,
		stream: stream,
		lastSubID: &MsgID{
			MsTimestamp: t.UnixNano() / 1e6,
			Seq:         0,
		},
	}, nil
}

func (r *RedisMessager) Pub(ctx context.Context, data []byte) (msgID string, err error) {
	item := &redis.XAddArgs{
		Stream: r.stream,
		Values: map[string]interface{}{
			"data": data,
		},
		Approx: true,
	}
	// if r.lastPubID != nil && r.lastPubID.Gt(new(MsgID)) {
	// 	// 移除队列中超出时间限制的消息，但用的时间是redis服务器的（从消息id中解析），避免本地时钟不准
	// 	item.MinID = r.lastPubID.Add(-MsgRetainDur).String()
	// }
	msgID, err = r.cli.XAdd(ctx, item).Result()
	if err != nil {
		return "", errors.Wrapf(err, "redis xadd, args: %+v", item)
	}

	// 处理消息水位
	newPubID, err := ParseMsgID(msgID)
	if err != nil {
		return "", errors.Wrapf(err, "parse msgid %s", msgID)
	}
	r.lock.Lock()
	if r.lastPubID == nil || newPubID.Gt(r.lastPubID) {
		r.lastPubID = newPubID
	}
	r.lock.Unlock()

	return
}

// result: msgID -> data
func (r *RedisMessager) DupSub(ctx context.Context) (result map[string][]byte, err error) {
	args := &redis.XReadArgs{
		Streams: []string{r.stream},
		Count:   5,
		Block:   ReadBlockDur,
		ID:      r.lastSubID.String(),
	}

	data, err := r.cli.XRead(ctx, args).Result()
	if errors.Is(err, redis.Nil) {
		return make(map[string][]byte), nil
	}
	if err != nil {
		return nil, errors.Wrap(err, "redis xread")
	}
	if len(data) == 0 {
		return make(map[string][]byte), nil
	}

	// 更新消费水位
	for _, msg := range data[0].Messages {
		newSubID, err := ParseMsgID(msg.ID)
		if err != nil {
			return nil, errors.Wrapf(err, "parse msgid %s", msg.ID)
		}
		r.lock.Lock()
		if r.lastSubID == nil || newSubID.Gt(r.lastSubID) {
			r.lastSubID = newSubID
		}
		r.lock.Unlock()
	}

	// 处理消息
	result = make(map[string][]byte)
	for _, msg := range data[0].Messages {
		msgID := msg.ID
		var msgData []byte
		if data, ok := msg.Values["data"]; !ok {
			logc.Errorf(ctx, "redis xread msg %s has no data field", msgID)
		} else if tmp, ok := data.([]byte); ok {
			msgData = tmp
		} else if tmp, ok := data.(string); ok {
			msgData = []byte(tmp)
		} else {
			logc.Errorf(ctx, "redis xread msg %s data field not support: %v", msgID, reflect.TypeOf(data))
		}
		result[msgID] = msgData
	}
	return
}

func (r *RedisMessager) Ack(ctx context.Context, msgID string) error {
	// xread不需要ack，只需要消费者自己维护消费水位
	return nil
}
