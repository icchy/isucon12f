package main

import (
	"net/http"

	"github.com/gorilla/securecookie"
	"github.com/gorilla/sessions"
)

type XSessionStore struct {
	Codecs  []securecookie.Codec
	Options *sessions.Options
}

func NewXSessionStore(keyPairs ...[]byte) *XSessionStore {
	cs := &XSessionStore{
		Codecs: securecookie.CodecsFromPairs(keyPairs...),
	}
	return cs
}

func (s *XSessionStore) Get(r *http.Request, name string) (*sessions.Session, error) {
	return sessions.GetRegistry(r).Get(s, name)
}

func (s *XSessionStore) New(r *http.Request, name string) (*sessions.Session, error) {
	session := sessions.NewSession(s, name)
	session.IsNew = true

	sessValue := r.Header.Get("x-session")
	if sessValue == "" {
		return session, nil
	}

	if err := securecookie.DecodeMulti(name, sessValue, &session.Values, s.Codecs...); err != nil {
		return session, err
	}
	session.IsNew = false
	return session, nil
}

func (s *XSessionStore) Save(r *http.Request, w http.ResponseWriter, session *sessions.Session) error {
	encoded, err := securecookie.EncodeMulti(session.Name(), session.Values, s.Codecs...)
	if err != nil {
		return err
	}
	w.Header().Set("x-session", encoded)
	return nil
}
