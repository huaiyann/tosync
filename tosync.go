package tosync

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/huaiyann/tosync/internal/messager"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logc"
)

var defaultClient *Client

type ReqI interface {
	SetCallbackURL(url string)
	GetCallbackURL() string
}

func Init(redisCli *redis.Client, cfg *Config) (err error) {
	if defaultClient != nil {
		err = errors.New("client already inited")
		return
	}
	if err := cfg.Validate(); err != nil {
		err = errors.Wrap(err, "validate config")
		return err
	}

	msger, err := messager.NewRedisMessager(redisCli, cfg.Stream)
	if err != nil {
		err = errors.Wrap(err, "new redis messager")
		return
	}
	defaultClient = &Client{
		messager:    msger,
		waiters:     make(map[string]*WaiterInfo),
		callbackURL: cfg.CallbackURL,
		maxSize:     cfg.MaxCallbackBytes,
		timeout:     time.Second * time.Duration(cfg.TimeoutSeconds),
	}
	go defaultClient.listen()
	return
}

func ToSync[Req ReqI, CallbackData any](ctx context.Context, req Req, async func(context.Context, Req) error, opts ...*Option) (data CallbackData, err error) {
	opt := mergeOptions(opts...)

	var client *Client
	if opt.Client != nil {
		client = opt.Client
	} else {
		client = defaultClient
	}
	if client == nil {
		err = errors.New("client not inited")
		return
	}

	timeout := client.timeout
	if opt.Timeout > 0 {
		timeout = opt.Timeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	err = checkType[CallbackData]()
	if err != nil {
		err = errors.Wrap(err, "check CallbackData type")
		return
	}

	// 注册监听结果任务，包括会调整req内的callbackURL
	waitInfo, err := client.Regist(req)
	if err != nil {
		err = errors.Wrap(err, "regist req")
		return
	}
	defer client.Release(waitInfo)

	// 提交异步任务
	err = async(ctx, req)
	if err != nil {
		err = errors.Wrap(err, "exec async func")
		return
	}

	buf, _ := json.Marshal(req)
	logc.Infof(ctx, "[ToSync] task submitted, async id %s, param %s", waitInfo.AsyncID, buf)

	// 等待监听到的异步回调结果
	select {
	case <-ctx.Done():
		err = ctx.Err()
		if err != nil {
			return
		}
	case callbackInfo := <-waitInfo.ResultC:
		tmp := newParam[CallbackData]()
		err = client.messager.Ack(ctx, callbackInfo.MsgID)
		if err != nil {
			err = errors.Wrap(err, "ack")
			return
		}
		err = json.Unmarshal(callbackInfo.Body, tmp.Interface())
		if err != nil {
			err = errors.Wrap(err, "unmarshal callback body")
			return
		}
		data = tmp.Elem().Interface().(CallbackData)
	}
	return
}

func CallbackHandler(ctx context.Context, r *http.Request) (err error) {
	client := defaultClient
	if client == nil {
		err = errors.New("client not inited")
		return
	}
	return client.CallbackHandler(ctx, r)
}
