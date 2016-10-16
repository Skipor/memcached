package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"

	"github.com/skipor/memcached"
	"github.com/skipor/memcached/internal/util"
	"github.com/skipor/memcached/log"
)

func main() {
	// TODO pprof monitoring on configurable port
	conf := config()
	s, err := memcached.NewServer(conf)
	s.Log.Fatal("Can't start server:", err)

	s.Log.Info("Serve on %s.", s.Addr)
	err = s.ListenAndServe()
	s.Log.Fatal("Serve error: ", err)
}

const usage = `
Config values merge rules:
1) config file value overrides default
2) command line value overrides any
Options:
`

func DefaultInputConfig() *InputConfig {
	return &InputConfig{
		Port:           11211,
		Host:           "",
		LogDestination: "stderr",
		LogLevel:       "info",
		CacheSize:      "64m",
		MaxItemSize:    "1m",
	}
}

type InputConfig struct {
	Port           int    `json:"port"`
	Host           string `json:"host"`
	LogDestination string `json:"log-destination"` // Stdout, stderr, or filepath.
	LogLevel       string `json:"log-level"`
	// Size values 10g, 128m, 1024k, 1000000b
	CacheSize   string `json:"cache-size"`
	MaxItemSize string `json:"max-item-size"`
}

// config parses command flags, reads config file if any, returns merged config.
// Config values merge rules:
// 1) config file value overrides default
// 2) command line value overrides any
func config() memcached.Config {
	l := log.NewLogger(log.DebugLevel, os.Stderr)
	flg := parseFlags()
	fileConf := DefaultInputConfig()
	if flg.ConfigPath != "" {
		data, err := ioutil.ReadFile(flg.ConfigPath)
		if err != nil {
			l.Fatal("Config file read error: ", err)
		}
		err = json.Unmarshal(data, fileConf)
		if err != nil {
			l.Fatal("Config parse error: ", err)
		}
	}
	// TODO  validate that there is no AOF options without AOF file name
	mergeConfigs(fileConf, &flg.InputConfig)
	return parseConfig(l, fileConf)
}

//func validateConf(conf Config) error {
//	confWithoutName := conf.AOF
//	confWithoutName.Name = ""
//	if conf.FixCorruptedAOF != true || util.IsZero(confWithoutName) {
//		return stackerr.New("Persistence not enabled, but passed some persistence options.\n" +
//			"Probably you want pass AOF name.")
//	}
//	return nil
//}

func parseConfig(l log.Logger, fileConf *InputConfig) memcached.Config {
	parsed := memcached.Config{}
	var err error
	parsed.LogDestination, err = logDestination(fileConf.LogDestination)
	if err != nil {
		l.Fatal("Log destination open error:", err)
	}
	parsed.Cache.Size, err = parseSize(fileConf.CacheSize)
	if err != nil {
		l.Fatal("Cache size parse error:", err)
	}
	parsed.MaxItemSize, err = parseSize(fileConf.MaxItemSize)
	if err != nil {
		l.Fatal("Max item size parse error:", err)
	}
	if parsed.MaxItemSize > memcached.MaxItemSize {
		l.Fatal("Too large max item size.")
	}
	parsed.LogLevel, err = log.LevelFromString(fileConf.LogLevel)
	if err != nil {
		l.Fatal("Log level parse error: ", err)
	}
	parsed.Addr = net.JoinHostPort(fileConf.Host, strconv.Itoa(fileConf.Port))
	return parsed
}

type Flags struct {
	ConfigPath string
	InputConfig
}

// NOTE: without "only stdlib" constraint I would
// github.com/spf13/viper and with custom github.com/mitchellh/mapstructure decode hooks
// for configuration and github.com/spf13/cobra for CLI.
// NOTE: for simplicity configure only from file.
func parseFlags() Flags {
	var f Flags
	flag.StringVar(&f.ConfigPath, "config", "", "path to json config")

	def := DefaultInputConfig()
	usage := func(usage string, defVal interface{}) string {
		if _, ok := defVal.(string); ok {
			usage += fmt.Sprintf(" (default %q)", defVal)
		} else {
			usage += fmt.Sprintf(" (default %v)", defVal)
		}
		return usage
	}
	flag.StringVar(&f.Host, "host", "", usage("host address to bind", def.Host))
	flag.IntVar(&f.Port, "port", 0, usage("port num", def.Port))
	flag.StringVar(&f.LogDestination, "log-destination", "", usage("log destination: stederr, stdout or file path", def.LogDestination))
	flag.StringVar(&f.LogLevel, "log-level", "", usage("log level: debug, info, warn, error, fatal", def.LogLevel))
	flag.StringVar(&f.CacheSize, "cache-size", "", usage("cache size: 2g, 64m", def.CacheSize))
	flag.StringVar(&f.MaxItemSize, "max-item-size", "", usage("max item size: 10m, 1024k", def.MaxItemSize))
	flag.Parse()
	return f
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

// mergeConfigs overwrite def values with non zero override values
// WARN: not recursive now.
func mergeConfigs(def, override *InputConfig) {
	defVal := reflect.ValueOf(def).Elem()
	overrideVal := reflect.ValueOf(override).Elem()
	for i, end := 0, defVal.NumField(); i < end; i++ {
		overrideVal := overrideVal.Field(i)
		if util.IsZeroVal(overrideVal) {
			defVal.Field(i).Set(overrideVal)
		}
	}
}

func saveDefaultConf() {
	data, err := json.Marshal(DefaultInputConfig())
	if err != nil {
		panic(err)
	}
	err = ioutil.WriteFile("./config.json", data, 0666)
	if err != nil {
		panic(err)
	}
}

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "%s", usage)
		flag.PrintDefaults()
	}
}
