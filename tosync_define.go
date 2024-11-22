package tosync

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/huaiyann/tosync/internal/signature"
	"github.com/pkg/errors"
	"github.com/zeromicro/go-zero/core/logc"
)

var ErrInvalidSign = errors.New("invalid sign")

type WaiterInfo struct {
	AsyncID string
	State   string
	ResultC chan *CallbackInfoParsed
}

type CallbackInfo struct {
	AsyncID    string `json:"async_id"`
	Base64Body string `json:"base64_body"`
}

type CallbackInfoParsed struct {
	MsgID string // 用于消息队列的ack
	Body  []byte
}

type Client struct {
	lock        sync.RWMutex
	messager    Messager
	waiters     map[string]*WaiterInfo
	callbackURL string
	maxSize     int64 // callback body的最大size
	timeout     time.Duration
}

func (c *Client) CallbackHandler(ctx context.Context, r *http.Request) (err error) {
	asyncID := r.URL.Query().Get("async_id")
	sign := r.URL.Query().Get("sign")
	random := r.URL.Query().Get("random")

	// 校验sign
	if !signature.CheckSign(asyncID, random, sign) {
		return ErrInvalidSign
	}

	reader := io.LimitReader(r.Body, c.maxSize+1)
	buf, err := io.ReadAll(reader)
	if err != nil {
		err = errors.Wrap(err, "read body")
		return
	}
	if int64(len(buf)) > c.maxSize {
		err = errors.Errorf("body limited to %d bytes", c.maxSize)
		return
	}

	callbackInfo := new(CallbackInfo)
	callbackInfo.AsyncID = asyncID
	callbackInfo.Base64Body = base64.StdEncoding.EncodeToString(buf)
	infoBuf, err := json.Marshal(callbackInfo)
	if err != nil {
		err = errors.Wrap(err, "marshal callbackInfo")
		return
	}

	msgID, err := c.messager.Pub(ctx, infoBuf)
	if err != nil {
		err = errors.Wrap(err, "pub")
		return
	}
	logc.Infof(ctx, "[ToSync] get callback data %s, async_id %s, pub to msgID %s", buf, asyncID, msgID)
	return
}

func (c *Client) listen() {
	for {
		ctx := context.Background()
		data, err := c.messager.DupSub(ctx)
		if err != nil {
			logc.Errorf(ctx, "[ToSync] sub error: %v", err)
			time.Sleep(time.Millisecond * 100)
			continue
		}
		for msgID, buf := range data {
			info, err := c.processMsg(msgID, buf)
			if err != nil {
				logc.Errorf(ctx, "[ToSync] process msg %s, error: %v", msgID, err)
			} else {
				logc.Infof(ctx, "[ToSync] process msg %s, info: %s", msgID, info)
			}
			err = c.messager.Ack(ctx, msgID)
			if err != nil {
				logc.Errorf(ctx, "[ToSync] ack msg %s, error: %v", msgID, err)
			}
		}
	}
}

func (c *Client) processMsg(msgID string, buf []byte) (string, error) {
	callbackInfo := new(CallbackInfo)
	err := json.Unmarshal(buf, callbackInfo)
	if err != nil {
		return "", errors.Wrap(err, "unmarshal callbackInfo")
	}
	callbackBody, err := base64.StdEncoding.DecodeString(callbackInfo.Base64Body)
	if err != nil {
		return "", errors.Wrap(err, "decode base64 body")
	}

	waitInfo, ok := c.waiters[callbackInfo.AsyncID]
	if !ok {
		return "AsyncID not registed in this client", nil
	} else {
		select {
		case waitInfo.ResultC <- &CallbackInfoParsed{
			MsgID: msgID,
			Body:  callbackBody,
		}:
			return "success", nil
		default:
			return "duplicated msg and channel full", nil
		}
	}
}

func (c *Client) Regist(req ReqI) (*WaiterInfo, error) {
	// 不能带有callbackURL，因为要走统一的
	if req.GetCallbackURL() != "" {
		return nil, errors.New("callbackURL should be empty")
	}

	//基于统一的callbackURL，拼接taskID、random、sign到callbackURL中
	asyncID := uuid.NewString()
	random, sign := signature.GenSign(asyncID)
	newURL, err := url.Parse(c.callbackURL)
	if err != nil {
		return nil, errors.Wrapf(err, "parse callbackURL %s", c.callbackURL)
	}
	values := newURL.Query()
	values.Add("random", fmt.Sprintf("%d", random))
	values.Add("sign", sign)
	values.Add("async_id", asyncID)
	newURL.RawQuery = values.Encode()
	newURLStr := newURL.String()
	req.SetCallbackURL(newURLStr)
	if tmp := req.GetCallbackURL(); tmp != newURLStr {
		return nil, errors.Errorf("callbackURL should be %s but %s", newURLStr, tmp)
	}

	info := &WaiterInfo{
		AsyncID: asyncID,
		ResultC: make(chan *CallbackInfoParsed, 1),
	}
	c.lock.Lock()
	c.waiters[asyncID] = info
	c.lock.Unlock()

	return info, nil
}

func (c *Client) Release(info *WaiterInfo) {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.waiters, info.AsyncID)
}

// 返回初始化好的*T类型的reflect.Value
func newParam[T any]() reflect.Value {
	var null T
	tt := reflect.TypeOf(null)
	return reflect.New(tt)
}

// // 支持的T类型：map、slice、array、struct，及其指针。
// // 不支持：interface{}
func checkType[T any]() error {
	var null T
	tt := reflect.TypeOf(null)
	var pointetFlag string
	if tt.Kind() == reflect.Pointer {
		tt = tt.Elem()
		pointetFlag = "*"
	}
	switch tt.Kind() {
	case reflect.Map, reflect.Slice, reflect.Array, reflect.Struct:
		return nil
	default:
		return errors.Errorf("unexpected type: %s%v", pointetFlag, tt)
	}
}
