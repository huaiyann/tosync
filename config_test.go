package tosync

import "testing"

func TestConfig_Validate(t *testing.T) {
	cfg := Config{
		CallbackURL:      "http://mhapi.com?",
		MaxCallbackBytes: 1,
		Stream:           "123",
		TimeoutSeconds:   10,
	}
	err := cfg.Validate()
	if err != nil {
		t.Fatal(err)
	}

	// 必须是合法url
	{
		tmp := cfg
		tmp.CallbackURL = "mhapi.com"
		err = tmp.Validate()
		if err == nil {
			t.Fatal("expect error")
		}
	}

	// url不能为空
	{
		tmp := cfg
		tmp.CallbackURL = ""
		err = tmp.Validate()
		if err == nil {
			t.Fatal("expect error")
		}
	}

	// MaxCallbackBytes必须大于0
	{
		tmp := cfg
		tmp.MaxCallbackBytes = 0
		err = tmp.Validate()
		if err == nil {
			t.Fatal("expect error")
		}

		tmp.MaxCallbackBytes = -1
		err = tmp.Validate()
		if err == nil {
			t.Fatal("expect error")
		}
	}

	// Stream必须有值
	{
		tmp := cfg
		tmp.Stream = ""
		err = tmp.Validate()
		if err == nil {
			t.Fatal("expect error")
		}
	}

	// timeout必须有值
	{
		tmp := cfg
		tmp.TimeoutSeconds = 0
		err = tmp.Validate()
		if err == nil {
			t.Fatal("expect error")
		}
	}
}
