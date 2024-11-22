package messager

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/huaiyann/congroup/v2"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

func TestRedisMessagerPubSub(t *testing.T) {
	ctx := context.Background()
	cli := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	err := cli.Ping(ctx).Err()
	if err != nil {
		t.Fatalf("ping redis failed: %v", err)
	}

	cgMultiTest := congroup.New(ctx)
	for i := 0; i < 50; i++ {
		cgMultiTest.Add(func(ctx context.Context) error {
			tmpStream := "test_stream_" + uuid.NewString()
			defer cli.Expire(ctx, tmpStream, time.Hour)
			t.Logf("tmp stream: %s", tmpStream)
			msger, err := NewRedisMessager(cli, tmpStream)
			if err != nil {
				return errors.Wrap(err, "new redis messager failed")
			}

			// 消费消息
			getMsg := make(map[string][]byte)
			ReadBlockDur = time.Second
			cg := congroup.New(ctx)
			cg.Add(func(ctx context.Context) error {
				for nullCnt := 0; nullCnt < 2; {
					data, err := msger.DupSub(ctx)
					if err != nil {
						return errors.Wrap(err, "dup sub failed")
					}
					for msgID, msgData := range data {
						getMsg[msgID] = msgData
					}
					if len(data) == 0 {
						t.Log("get no msg")
						nullCnt++
					}
				}
				return nil
			})

			wantMsg := make(map[string][]byte)
			// 生产消息
			for i := 0; i < 20; i++ {
				data := []byte(uuid.NewString())
				msgID, err := msger.Pub(ctx, data)
				if err != nil {
					return errors.Wrap(err, "pub failed")
				}
				wantMsg[msgID] = data
			}

			err = cg.Wait()
			if err != nil {
				t.Fatal(err)
			}

			// 对比消息
			if !reflect.DeepEqual(wantMsg, getMsg) {
				return errors.Errorf("want msg cnt %v, get msg cnt %v", len(wantMsg), len(getMsg))
			}
			return nil
		})
	}
	err = cgMultiTest.Wait()
	if err != nil {
		t.Fatal(err)
	}
}

func TestRedisMessagerTrim(t *testing.T) {
	ctx := context.Background()
	cli := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	err := cli.Ping(ctx).Err()
	if err != nil {
		t.Fatalf("ping redis failed: %v", err)
	}

	MsgRetainDur = time.Second
	tmpStream := "test_stream_" + uuid.NewString()
	defer cli.Expire(ctx, tmpStream, time.Hour)
	t.Logf("tmp stream: %s", tmpStream)
	msger, err := NewRedisMessager(cli, tmpStream)
	if err != nil {
		t.Fatalf("new redis messager failed: %v", err)
	}

	// 生产200条消息
	for i := 0; i < 200; i++ {
		_, err = msger.Pub(ctx, []byte(uuid.NewString()))
		if err != nil {
			t.Fatalf("pub failed: %v", err)
		}
	}
	// 长度应该是200
	len, err := cli.XLen(ctx, tmpStream).Result()
	if err != nil {
		t.Fatalf("get stream len failed: %v", err)
	}
	if len != 200 {
		t.Fatalf("want stream len 200, get %d", len)
	}

	// 等待消息过期
	time.Sleep(MsgRetainDur + time.Second)
	// 再生产2条消息
	for i := 0; i < 2; i++ {
		_, err = msger.Pub(ctx, []byte(uuid.NewString()))
		if err != nil {
			t.Fatalf("pub failed: %v", err)
		}
	}

	// 因为是非精确修剪，长度应该小于100
	len, err = cli.XLen(ctx, tmpStream).Result()
	if err != nil {
		t.Fatalf("get stream len failed: %v", err)
	}
	if len >= 100 {
		t.Fatalf("want stream len < 100, get %d", len)
	}
}
