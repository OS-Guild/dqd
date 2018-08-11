package utils

import (
	"io/ioutil"
	"os"
	"strconv"

	"github.com/rs/zerolog/log"
)

var logger = log.With().Str("scope", "Main").Logger()

func GetenvInt(s string, defaultValue int64) int64 {
	value := os.Getenv(s)
	i, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return defaultValue
	}
	return i
}

func GetenvRequired(s string) string {
	value := os.Getenv(s)
	if value == "" {
		logger.Fatal().Msg("Env var " + s + " is required")
	}
	return value
}

func GetenvOrFile(varName, fileVarName string, required bool) string {
	value := os.Getenv(varName)
	if value != "" {
		return value
	}
	fileName := os.Getenv(fileVarName)
	dat, err := ioutil.ReadFile(fileName)
	if err == nil {
		return string(dat)
	}
	if required {
		logger.Fatal().Msg("Env var " + varName + " or " + fileVarName + " is required")
	}
	return ""
}
