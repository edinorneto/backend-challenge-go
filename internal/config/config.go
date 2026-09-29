package config

import "os"

type Config struct {
	DatabaseURL string
	HTTPAddr    string
}

func Load() Config {
	return Config{
		DatabaseURL: getEnv(
			"DATABASE_URL",
			"postgres://postgres:postgres@localhost:5432/betting",
		),
		HTTPAddr: getEnv(
			"HTTP_ADDR",
			":8080",
		),
	}
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)

	if value == "" {
		return fallback
	}

	return value
}