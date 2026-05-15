package pagination

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"sync"
)

type Cursor struct {
	Op  string `json:"op"`
	Idx int    `json:"idx"`
}

var (
	keyOnce sync.Once
	hmacKey []byte
)

func initKey() {
	keyOnce.Do(func() {
		id := os.Getenv("JAISCLOUD_INSTANCE_ID")
		if id == "" {
			id = "jaiscloud-default-pagination-key"
		}
		h := sha256.Sum256([]byte("pagination:" + id))
		hmacKey = h[:]
	})
}

func Encode(c Cursor) string {
	initKey()
	data, _ := json.Marshal(c)
	mac := hmac.New(sha256.New, hmacKey)
	mac.Write(data)
	sig := mac.Sum(nil)
	combined := append(sig, data...)
	return base64.RawURLEncoding.EncodeToString(combined)
}

func Decode(token, op string) (Cursor, error) {
	initKey()
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(raw) < 32 {
		return Cursor{}, errors.New("InvalidNextToken")
	}
	sig, data := raw[:32], raw[32:]
	mac := hmac.New(sha256.New, hmacKey)
	mac.Write(data)
	if !hmac.Equal(mac.Sum(nil), sig) {
		return Cursor{}, errors.New("InvalidNextToken")
	}
	var c Cursor
	if err := json.Unmarshal(data, &c); err != nil {
		return Cursor{}, errors.New("InvalidNextToken")
	}
	if c.Op != op {
		return Cursor{}, errors.New("InvalidNextToken: wrong operation")
	}
	return c, nil
}

// Paginate slices items using an opaque token. Returns (page, nextToken, error).
// max <= 0 means return all items.
func Paginate[T any](items []T, max int, token, op string) ([]T, string, error) {
	start := 0
	if token != "" {
		c, err := Decode(token, op)
		if err != nil {
			return nil, "", err
		}
		start = c.Idx
	}
	if start >= len(items) {
		return []T{}, "", nil
	}
	if max <= 0 || max > len(items) {
		max = len(items)
	}
	end := start + max
	if end > len(items) {
		end = len(items)
	}
	page := items[start:end]
	next := ""
	if end < len(items) {
		next = Encode(Cursor{Op: op, Idx: end})
	}
	return page, next, nil
}
