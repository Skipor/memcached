package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/facebookgo/stackerr"

	"github.com/skipor/memcached"
	"github.com/skipor/memcached/internal/util"
	"github.com/skipor/memcached/log"
)

const RotateSizeCoef = 3 //TODO make configurable

func Parse(conf Config) (mconf memcached.Config, err error) {
	mconf.LogDestination, err = logDestination(conf.LogDestination)
	if err != nil {
		err = stackerr.Newf("Log destination open error: %v", err)
		return
	}
	mconf.Cache.Size, err = parseSize(conf.CacheSize)
	if err != nil {
		err = stackerr.Newf("Cache size parse error: %v", err)
		return
	}
	mconf.MaxItemSize, err = parseSize(conf.MaxItemSize)
	if err != nil {
		err = stackerr.Newf("Max item size parse error: %v", err)
		return
	}
	if mconf.MaxItemSize > memcached.MaxItemSize {
		err = stackerr.Newf("Too large max item size.")
		return
	}
	mconf.LogLevel, err = log.LevelFromString(conf.LogLevel)
	if err != nil {
		err = stackerr.Newf("Log level parse error: %v", err)
		return
	}
	mconf.AOF.Name = conf.AOF.Name
	mconf.FixCorruptedAOF = conf.AOF.FixCorrupted
	var bufSize int64
	bufSize, err = parseSize(conf.AOF.BufSize)
	mconf.AOF.BufSize = int(bufSize)
	if err != nil {
		err = stackerr.Newf("BufSize parse error: %v", err)
		return
	}
	mconf.AOF.RotateSize = mconf.Cache.Size * RotateSizeCoef
	mconf.Addr = net.JoinHostPort(conf.Host, strconv.Itoa(conf.Port))
	return
}

func Default() *Config {
	return &Config{
		Port:           11211,
		Host:           "",
		LogDestination: "stderr",
		LogLevel:       "info",
		CacheSize:      "64m",
		MaxItemSize:    "1m",
		AOF: AOFConfig{
			BufSize: "4k",
		},
	}
}

type Config struct {
	Port           int    `json:"port,omitempty"`
	Host           string `json:"host,omitempty"`
	LogDestination string `json:"log-destination,omitempty"` // Stdout, stderr, or filepath.
	LogLevel       string `json:"log-level,omitempty"`
	// Size values 10g, 128m, 1024k, 1000000b
	CacheSize   string    `json:"cache-size,omitempty"`
	MaxItemSize string    `json:"max-item-size,omitempty"`
	AOF         AOFConfig `json:"aof,omitempty"`
}

type AOFConfig struct {
	Name         string        `json:"name,omitempty"`
	Sync         time.Duration `json:"sync,omitempty"`
	BufSize      string        `json:"buf-size,omitempty"`
	FixCorrupted bool          `json:"fix-corrupted,omitempty"`
}

func Merge(def, override *Config) {
	defAof := def.AOF
	merge(def, override)

	// HACK: manual recursion. Some third party high level reflection package should be used here.
	merge(&defAof, &override.AOF)
	def.AOF = defAof
}
func merge(def, override interface{}) {
	defVal := reflect.ValueOf(def).Elem()
	overrideVal := reflect.ValueOf(override).Elem()
	for i, end := 0, defVal.NumField(); i < end; i++ {

		overrideVal := overrideVal.Field(i)
		fmt.Sprintf("%v", overrideVal)
		if !util.IsZeroVal(overrideVal) {
			defVal.Field(i).Set(overrideVal)
		}
	}
}

func Marshal(conf *Config) []byte {
	data, err := json.Marshal(conf)
	if err != nil {
		panic(err)
	}
	return data
}

func parseSize(s string) (size int64, err error) {
	if len(s) < 2 {
		err = errors.New("Invalid size format.")
		return
	}
	sep := len(s) - 1
	sizeStr := s[:sep]
	exponentStr := s[sep:]
	var exponent uint32
	switch strings.ToLower(exponentStr) {
	case "b":
		exponent = 0
	case "k":
		exponent = 10
	case "m":
		exponent = 20
	case "g":
		exponent = 30
	default:
		err = errors.New("Invalid exponent. Only 'b', 'k', 'm', 'g' allowed.")
		return
	}
	size, err = strconv.ParseInt(sizeStr, 10, 31)
	if err != nil {
		err = fmt.Errorf("Size parse error: %s", err)
		return
	}
	size <<= exponent
	return
}

func logDestination(dest string) (w io.Writer, err error) {
	switch strings.ToLower(dest) {
	case "stderr":
		w = os.Stderr
	case "stdout":
		w = os.Stdout
	default:
		w, err = os.OpenFile(dest, os.O_APPEND|os.O_CREATE, 0)
	}
	return
}
