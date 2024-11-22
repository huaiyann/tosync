package tosync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/huaiyann/congroup/v2"
	"github.com/huaiyann/tosync/internal/signature"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

// 测试用参数
type TestReq struct {
	CallbackURL string
}

func (t *TestReq) SetCallbackURL(url string) {
	t.CallbackURL = url
}

func (t *TestReq) GetCallbackURL() string {
	return t.CallbackURL
}

// 测试用返回值
type TestCallbackData struct {
	Msg string `json:"msg"`
}

// 测试用http server
var callbackPort int
var startOnce sync.Once

func startServer() {
	callbackPort = 10000 + rand.Intn(10000)
	http.HandleFunc("/callback", func(w http.ResponseWriter, r *http.Request) {
		err := CallbackHandler(r.Context(), r)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	http.ListenAndServe(fmt.Sprintf(":%d", callbackPort), nil)
}

// 验证正常逻辑
func TestToSync(t *testing.T) {
	go startOnce.Do(startServer)
	ctx := context.Background()
	cli := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	err := cli.Ping(ctx).Err()
	if err != nil {
		t.Fatalf("ping redis failed: %v", err)
	}
	defaultClient = nil
	err = Init(cli, &Config{
		CallbackURL:      fmt.Sprintf("http://localhost:%d/callback", callbackPort),
		MaxCallbackBytes: 1024 * 1024,
		Stream:           "to_sync_test",
		TimeoutSeconds:   10,
	})
	if err != nil {
		t.Fatalf("init tosync failed: %v", err)
	}

	cg := congroup.New(ctx)

	c := make(chan map[string]string, 10)
	// 模拟回调
	cg.Add(func(ctx context.Context) error {
		for data := range c {
			callbcak := data["callback"]
			var buf []byte
			if data["type"] == "slice" {
				buf, err = json.Marshal([]string{data["msg"]})
				if err != nil {
					return errors.Wrap(err, "marshal data")
				}
			} else {
				buf, err = json.Marshal(&TestCallbackData{
					Msg: data["msg"],
				})
				if err != nil {
					return errors.Wrap(err, "marshal data")
				}
			}
			resp, err := http.Post(callbcak, "application/json", bytes.NewBuffer(buf))
			if err != nil {
				return errors.Wrap(err, "post callback")
			}
			if resp.StatusCode != http.StatusOK {
				return errors.New("callback failed")
			}
		}
		return nil
	})

	// 异步转同步, callback值类型struct
	cg.Add(func(ctx context.Context) error {
		msg := uuid.NewString()
		data, err := ToSync[*TestReq, TestCallbackData](ctx, &TestReq{}, func(ctx context.Context, req *TestReq) error {
			c <- map[string]string{
				"callback": req.GetCallbackURL(),
				"msg":      msg,
			}
			return nil
		})
		if err != nil {
			t.Fatalf("tosync failed: %v", err)
		}
		if data.Msg != msg {
			t.Fatalf("want %s, get %s", msg, data.Msg)
		}
		return nil
	})
	// 异步转同步, callback *struct
	cg.Add(func(ctx context.Context) error {
		msg := uuid.NewString()
		data, err := ToSync[*TestReq, *TestCallbackData](ctx, &TestReq{}, func(ctx context.Context, req *TestReq) error {
			c <- map[string]string{
				"callback": req.GetCallbackURL(),
				"msg":      msg,
			}
			return nil
		})
		if err != nil {
			t.Fatalf("tosync failed: %v", err)
		}
		if data.Msg != msg {
			t.Fatalf("want %s, get %s", msg, data.Msg)
		}
		return nil
	})
	// 异步转同步, callback map
	cg.Add(func(ctx context.Context) error {
		msg := uuid.NewString()
		data, err := ToSync[*TestReq, map[string]string](ctx, &TestReq{}, func(ctx context.Context, req *TestReq) error {
			c <- map[string]string{
				"callback": req.GetCallbackURL(),
				"msg":      msg,
			}
			return nil
		})
		if err != nil {
			t.Fatalf("tosync failed: %v", err)
		}
		if data["msg"] != msg {
			t.Fatalf("want %s, get %s", msg, data["msg"])
		}
		return nil
	})
	// 异步转同步, callback *map
	cg.Add(func(ctx context.Context) error {
		msg := uuid.NewString()
		data, err := ToSync[*TestReq, *map[string]string](ctx, &TestReq{}, func(ctx context.Context, req *TestReq) error {
			c <- map[string]string{
				"callback": req.GetCallbackURL(),
				"msg":      msg,
			}
			return nil
		})
		if err != nil {
			t.Fatalf("tosync failed: %v", err)
		}
		if (*data)["msg"] != msg {
			t.Fatalf("want %s, get %s", msg, (*data)["msg"])
		}
		return nil
	})
	// 异步转同步, callback slice
	cg.Add(func(ctx context.Context) error {
		msg := uuid.NewString()
		data, err := ToSync[*TestReq, []string](ctx, &TestReq{}, func(ctx context.Context, req *TestReq) error {
			c <- map[string]string{
				"callback": req.GetCallbackURL(),
				"msg":      msg,
				"type":     "slice",
			}
			return nil
		})
		if err != nil {
			t.Fatalf("tosync failed: %v", err)
		}
		if tmp := data[0]; tmp != msg {
			t.Fatalf("want %s, get %s", msg, tmp)
		}
		return nil
	})
	// 异步转同步, callback *slice
	cg.Add(func(ctx context.Context) error {
		msg := uuid.NewString()
		data, err := ToSync[*TestReq, *[]string](ctx, &TestReq{}, func(ctx context.Context, req *TestReq) error {
			c <- map[string]string{
				"callback": req.GetCallbackURL(),
				"msg":      msg,
				"type":     "slice",
			}
			return nil
		})
		if err != nil {
			t.Fatalf("tosync failed: %v", err)
		}
		if tmp := (*data)[0]; tmp != msg {
			t.Fatalf("want %s, get %s", msg, tmp)
		}
		return nil
	})
	// 异步转同步, callback array
	cg.Add(func(ctx context.Context) error {
		msg := uuid.NewString()
		data, err := ToSync[*TestReq, [1]string](ctx, &TestReq{}, func(ctx context.Context, req *TestReq) error {
			c <- map[string]string{
				"callback": req.GetCallbackURL(),
				"msg":      msg,
				"type":     "slice",
			}
			return nil
		})
		if err != nil {
			t.Fatalf("tosync failed: %v", err)
		}
		if tmp := data[0]; tmp != msg {
			t.Fatalf("want %s, get %s", msg, tmp)
		}
		return nil
	})
	// 异步转同步, callback *array
	cg.Add(func(ctx context.Context) error {
		msg := uuid.NewString()
		data, err := ToSync[*TestReq, *[1]string](ctx, &TestReq{}, func(ctx context.Context, req *TestReq) error {
			c <- map[string]string{
				"callback": req.GetCallbackURL(),
				"msg":      msg,
				"type":     "slice",
			}
			return nil
		})
		if err != nil {
			t.Fatalf("tosync failed: %v", err)
		}
		if tmp := (*data)[0]; tmp != msg {
			t.Fatalf("want %s, get %s", msg, tmp)
		}
		return nil
	})
	<-time.After(time.Second)
	close(c)
	err = cg.Wait()
	if err != nil {
		t.Fatal(err)
	}
}

// 验证两个client同时消费
func TestToSyncMultiClient(t *testing.T) {
	go startOnce.Do(startServer)
	ctx := context.Background()
	cli := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	err := cli.Ping(ctx).Err()
	if err != nil {
		t.Fatalf("ping redis failed: %v", err)
	}
	defaultClient = nil
	err = Init(cli, &Config{
		CallbackURL:      fmt.Sprintf("http://localhost:%d/callback", callbackPort),
		MaxCallbackBytes: 1024 * 1024,
		Stream:           "to_sync_test",
		TimeoutSeconds:   10,
	})
	if err != nil {
		t.Fatalf("init tosync failed: %v", err)
	}
	client1 := defaultClient
	defaultClient = nil
	err = Init(cli, &Config{
		CallbackURL:      fmt.Sprintf("http://localhost:%d/callback", callbackPort),
		MaxCallbackBytes: 1024 * 1024,
		Stream:           "to_sync_test",
		TimeoutSeconds:   10,
	})
	if err != nil {
		t.Fatalf("init tosync failed: %v", err)
	}
	client2 := defaultClient

	cg := congroup.New(ctx)

	c := make(chan map[string]string, 10)
	// 模拟回调
	cg.Add(func(ctx context.Context) error {
		for data := range c {
			callbcak := data["callback"]
			var buf []byte
			if data["type"] == "slice" {
				buf, err = json.Marshal([]string{data["msg"]})
				if err != nil {
					return errors.Wrap(err, "marshal data")
				}
			} else {
				buf, err = json.Marshal(&TestCallbackData{
					Msg: data["msg"],
				})
				if err != nil {
					return errors.Wrap(err, "marshal data")
				}
			}
			resp, err := http.Post(callbcak, "application/json", bytes.NewBuffer(buf))
			if err != nil {
				return errors.Wrap(err, "post callback")
			}
			if resp.StatusCode != http.StatusOK {
				return errors.New("callback failed")
			}
		}
		return nil
	})

	// client1
	for i := 0; i < 100; i++ {
		cg.Add(func(ctx context.Context) error {
			msg := uuid.NewString()
			data, err := ToSync[*TestReq, TestCallbackData](ctx, &TestReq{}, func(ctx context.Context, req *TestReq) error {
				c <- map[string]string{
					"callback": req.GetCallbackURL(),
					"msg":      msg,
				}
				return nil
			}, new(Option).SetClient(client1))
			if err != nil {
				t.Fatalf("tosync failed: %v", err)
			}
			if data.Msg != msg {
				t.Fatalf("want %s, get %s", msg, data.Msg)
			}
			return nil
		})
	}
	// client2
	for i := 0; i < 100; i++ {
		cg.Add(func(ctx context.Context) error {
			msg := uuid.NewString()
			data, err := ToSync[*TestReq, TestCallbackData](ctx, &TestReq{}, func(ctx context.Context, req *TestReq) error {
				c <- map[string]string{
					"callback": req.GetCallbackURL(),
					"msg":      msg,
				}
				return nil
			}, new(Option).SetClient(client2))
			if err != nil {
				t.Fatalf("tosync failed: %v", err)
			}
			if data.Msg != msg {
				t.Fatalf("want %s, get %s", msg, data.Msg)
			}
			return nil
		})
	}
	<-time.After(time.Second)
	close(c)
	err = cg.Wait()
	if err != nil {
		t.Fatal(err)
	}
}

// 验证超时
func TestToSyncTimeout(t *testing.T) {
	go startOnce.Do(startServer)
	ctx := context.Background()
	cli := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	err := cli.Ping(ctx).Err()
	if err != nil {
		t.Fatalf("ping redis failed: %v", err)
	}
	defaultClient = nil
	err = Init(cli, &Config{
		CallbackURL:      fmt.Sprintf("http://localhost:%d/callback", callbackPort),
		MaxCallbackBytes: 1024 * 1024,
		Stream:           "to_sync_test",
		TimeoutSeconds:   1,
	})
	if err != nil {
		t.Fatalf("init tosync failed: %v", err)
	}
	// 默认超时
	start := time.Now()
	_, err = ToSync[*TestReq, TestCallbackData](ctx, &TestReq{}, func(ctx context.Context, req *TestReq) error {
		return nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want %v, get %v", context.DeadlineExceeded, err)
	}
	sub := time.Since(start)
	if sub < time.Second || sub > time.Second+time.Millisecond*200 {
		t.Fatalf("want 1s, get %v", sub)
	}

	// 改短超时
	start = time.Now()
	timeout := time.Millisecond * 500
	_, err = ToSync[*TestReq, TestCallbackData](ctx, &TestReq{}, func(ctx context.Context, req *TestReq) error {
		return nil
	}, new(Option).SetTimeout(timeout))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want %v, get %v", context.DeadlineExceeded, err)
	}
	sub = time.Since(start)
	if sub < timeout || sub > timeout+time.Millisecond*200 {
		t.Fatalf("want %v, get %v", timeout, sub)
	}

	// 改长超时
	start = time.Now()
	timeout = time.Millisecond * 1500
	_, err = ToSync[*TestReq, TestCallbackData](ctx, &TestReq{}, func(ctx context.Context, req *TestReq) error {
		return nil
	}, new(Option).SetTimeout(timeout))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want %v, get %v", context.DeadlineExceeded, err)
	}
	sub = time.Since(start)
	if sub < timeout || sub > timeout+time.Millisecond*200 {
		t.Fatalf("want %v, get %v", timeout, sub)
	}
}

// 验证callback异常
func TestToSyncCallbackErr(t *testing.T) {
	go startOnce.Do(startServer)
	ctx := context.Background()
	cli := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	err := cli.Ping(ctx).Err()
	if err != nil {
		t.Fatalf("ping redis failed: %v", err)
	}
	defaultClient = nil
	err = Init(cli, &Config{
		CallbackURL:      fmt.Sprintf("http://localhost:%d/callback", callbackPort),
		MaxCallbackBytes: 10,
		Stream:           "to_sync_test",
		TimeoutSeconds:   10,
	})
	if err != nil {
		t.Fatalf("init tosync failed: %v", err)
	}
	// sign不对
	resp, err := http.Post(fmt.Sprintf("http://localhost:%d/callback", callbackPort),
		"application/json", strings.NewReader("12345678901"))
	if err != nil {
		t.Fatalf("post failed: %v", err)
	}
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("want %d, get %d", http.StatusInternalServerError, resp.StatusCode)
	}
	buf, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body failed: %v", err)
	}
	if !strings.Contains(string(buf), "invalid sign") {
		t.Fatalf("want %s, get %s", "invalid sign", string(buf))
	}

	// body过大
	random, sign := signature.GenSign("async_id")
	resp, err = http.Post(fmt.Sprintf("http://localhost:%d/callback?async_id=async_id&random=%d&sign=%s", callbackPort, random, sign),
		"application/json", strings.NewReader("12345678901"))
	if err != nil {
		t.Fatalf("post failed: %v", err)
	}
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("want %d, get %d", http.StatusInternalServerError, resp.StatusCode)
	}
	buf, err = io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body failed: %v", err)
	}
	if !strings.Contains(string(buf), "body limited to 10 bytes") {
		t.Fatalf("want %s, get %s", "body limited to 10 bytes", string(buf))
	}
}

// 验证不支持的类型
func TestToSyncNotSupportType(t *testing.T) {
	ctx := context.Background()
	cli := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	err := cli.Ping(ctx).Err()
	if err != nil {
		t.Fatalf("ping redis failed: %v", err)
	}
	defaultClient = nil
	err = Init(cli, &Config{
		CallbackURL:      fmt.Sprintf("http://localhost:%d/callback", callbackPort),
		MaxCallbackBytes: 10,
		Stream:           "to_sync_test",
		TimeoutSeconds:   10,
	})
	if err != nil {
		t.Fatalf("init tosync failed: %v", err)
	}
	_, err = ToSync[*TestReq, string](ctx, &TestReq{}, func(ctx context.Context, req *TestReq) error {
		return nil
	})
	if err == nil {
		t.Fatalf("expect error")
	}
	if want := "unexpected type: string"; !strings.Contains(err.Error(), want) {
		t.Fatalf("want %s, get %s", want, err.Error())
	}
}
