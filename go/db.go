package main

import (
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
)

func (h *Handler) testDBHosts() error {
	hosts := strings.Split(getEnv("ISUCON_DB_HOSTS", "127.0.0.1"), ",")

	for _, v := range hosts {
		dsn := fmt.Sprintf(
			"%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=true&loc=%s&multiStatements=%t&interpolateParams=true",
			getEnv("ISUCON_DB_USER", "isucon"),
			getEnv("ISUCON_DB_PASSWORD", "isucon"),
			v,
			getEnv("ISUCON_DB_PORT", "3306"),
			getEnv("ISUCON_DB_NAME", "isucon"),
			"Asia%2FTokyo",
			true,
		)
		dbx, err := sqlx.Open("mysql", dsn)
		defer dbx.Close()
		if err != nil {
			return err
		}
	}

	if len(h.DB) == 0 {
		return fmt.Errorf("no db connected")
	}

	return nil
}

func (h *Handler) connectDBHosts() error {
	hosts := strings.Split(getEnv("ISUCON_DB_HOSTS", "127.0.0.1"), ",")

	for _, v := range hosts {
		dsn := fmt.Sprintf(
			"%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=true&loc=%s&multiStatements=%t&interpolateParams=true",
			getEnv("ISUCON_DB_USER", "isucon"),
			getEnv("ISUCON_DB_PASSWORD", "isucon"),
			v,
			getEnv("ISUCON_DB_PORT", "3306"),
			getEnv("ISUCON_DB_NAME", "isucon"),
			"Asia%2FTokyo",
			true,
		)
		dbx, err := sqlx.Open("mysql", dsn)
		if err != nil {
			dbx.Close()
			return err
		}
		dbx.SetMaxIdleConns(64)
		dbx.SetMaxOpenConns(64)

		h.DB = append(h.DB, dbx)
	}

	if len(h.DB) == 0 {
		return fmt.Errorf("no db connected")
	}

	return nil
}

func connectDB(batch bool) (*sqlx.DB, error) {
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=true&loc=%s&multiStatements=%t&interpolateParams=true",
		getEnv("ISUCON_DB_USER", "isucon"),
		getEnv("ISUCON_DB_PASSWORD", "isucon"),
		getEnv("ISUCON_DB_HOST", "127.0.0.1"),
		getEnv("ISUCON_DB_PORT", "3306"),
		getEnv("ISUCON_DB_NAME", "isucon"),
		"Asia%2FTokyo",
		batch,
	)
	dbx, err := sqlx.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	dbx.SetMaxIdleConns(64)
	dbx.SetMaxOpenConns(64)
	return dbx, nil
}

func (h *Handler) getDB(userID int64) *sqlx.DB {
	idx := int(userID) % len(h.DB)
	return h.DB[idx]
}

func (h *Handler) getAdminDB() *sqlx.DB {
	return h.DB[0]
}
