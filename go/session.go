package main

import (
	"sync"

	"github.com/goccy/go-json"
	"github.com/gorilla/securecookie"
)

type XSession struct {
	s   *securecookie.SecureCookie
	mtx sync.Mutex
}

type JSONEncoder struct{}

func (e JSONEncoder) Serialize(src interface{}) ([]byte, error) {
	return json.Marshal(src)
}

func (e JSONEncoder) Deserialize(src []byte, dst interface{}) error {
	return json.Unmarshal(src, dst)
}

func NewXSession(hashKey, blockKey []byte) *XSession {
	s := securecookie.New(hashKey, blockKey)
	s.SetSerializer(JSONEncoder{})
	return &XSession{
		s: s,
	}
}

func (sess *XSession) encode(name string, data *map[string]int64) (string, error) {
	encoded, err := sess.s.Encode(name, *data)
	if err != nil {
		return "", err
	}
	return encoded, nil
}

func (sess *XSession) decode(name string, encoded *string, dest *map[string]int64) error {
	if err := sess.s.Decode(name, *encoded, &dest); err != nil {
		return err
	}

	return nil
}

func (sess *XSession) Get(name, encoded string, dest *map[string]int64) error {
	if err := sess.decode(name, &encoded, dest); err != nil {
		return err
	}
	return nil
}

func (sess *XSession) Save(name string, data *map[string]int64) (string, error) {
	return sess.encode(name, data)
}
