package main

import (
	"sync"

	"github.com/gofiber/fiber/v2"
	"github.com/gorilla/securecookie"
)

type XSession struct {
	s   *securecookie.SecureCookie
	mtx sync.Mutex
}

func NewXSession(hashKey, blockKey []byte) *XSession {
	return &XSession{
		s: securecookie.New(hashKey, blockKey),
	}
}

func (sess *XSession) encode(name string, data map[string]interface{}) (string, error) {
	encoded, err := sess.s.Encode(name, data)
	if err != nil {
		return "", err
	}
	return encoded, nil
}

func (sess *XSession) decode(name, encoded string, dest *map[string]interface{}) error {
	if err := sess.s.Decode(name, encoded, &dest); err != nil {
		return err
	}

	return nil
}

func (sess *XSession) Get(c *fiber.Ctx, name string, dest *map[string]interface{}) error {
	encoded := c.Get("x-session")
	if err := sess.decode(name, encoded, dest); err != nil {
		return err
	}
	return nil
}

func (sess *XSession) Save(name string, data map[string]interface{}) (string, error) {
	return sess.encode(name, data)
}
