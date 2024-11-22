package tosync

import (
	"net/url"
	"time"

	"github.com/go-playground/validator/v10"
)

type Config struct {
	CallbackURL      string `json:"callback_url" yaml:"callback_url" validate:"url"`              // 回调地址
	MaxCallbackBytes int64  `json:"max_callback_bytes" yaml:"max_callback_bytes" validate:"gt=0"` // 回调body限制
	Stream           string `json:"stream" yaml:"stream" validate:"gt=0"`                         // 回调stream key
	TimeoutSeconds   int    `json:"timeout_seconds" yaml:"timeout_seconds" validate:"gt=0"`       // 超时时间
}

func (c Config) Validate() error {
	// 校验callbackURL
	_, err := url.Parse(c.CallbackURL)
	if err != nil {
		return err
	}

	// 校验validate label
	return validator.New().Struct(c)
}

type Option struct {
	Client  *Client
	Timeout time.Duration
}

func (o *Option) SetClient(client *Client) *Option {
	o.Client = client
	return o
}

func (o *Option) SetTimeout(timeout time.Duration) *Option {
	o.Timeout = timeout
	return o
}

func mergeOptions(opts ...*Option) *Option {
	opt := &Option{}
	for _, o := range opts {
		if o.Client != nil {
			opt.Client = o.Client
		}
		if o.Timeout > 0 {
			opt.Timeout = o.Timeout
		}
	}
	return opt
}
