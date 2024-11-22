package signature

import (
	"crypto/md5"
	"crypto/sha512"
	"fmt"
	"math/rand"
)

const (
	signSalt = "GMswFuu1oyP3iNk9ceqiciLp6dBKs8hO"
)

func GenSign(asyncID string) (random int64, sign string) {
	random = rand.Int63n(10000000)
	data := fmt.Sprintf("%s_%d_%s", asyncID, random, signSalt)
	sign = fmt.Sprintf("%X", sha512.Sum512([]byte(data)))
	return
}

func CheckSign(asyncID string, random string, sign string) bool {
	data := fmt.Sprintf("%s_%s_%s", asyncID, random, signSalt)
	wantOld := fmt.Sprintf("%X", md5.Sum([]byte(data)))
	want := fmt.Sprintf("%X", sha512.Sum512([]byte(data)))
	return wantOld == sign || want == sign
}
