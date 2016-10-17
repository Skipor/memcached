package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/facebookgo/stackerr"
	"github.com/skipor/memcached"
	"github.com/skipor/memcached/cmd/memcached/config"
	"github.com/skipor/memcached/internal/tag"
	"github.com/skipor/memcached/internal/util"
	"github.com/skipor/memcached/log"
)

func main() {
	// TODO pprof monitoring on configurable port
	conf := loadConfigOrDie()
	s, err := memcached.NewServer(conf)
	if err != nil {
		log.NewLogger(log.FatalLevel, os.Stderr).Fatal("Can't start server: ", err)
	}
	if tag.Debug {
		s.Log.Warn("Using debug build. It has more runtime checks and large perfomance overhead.")
	}
	if tag.Race {
		s.Log.Info("Race detector is on.")
	}
	s.Log.Infof("Serve on %s.", s.Addr)
	err = s.ListenAndServe()
	s.Log.Fatal("Serve error: ", err)
}

const usage = `
Config values merge rules:
1) config file value overrides default
2) command line value overrides any
Options:
`

// config parses command flags, reads config file if any, returns merged config.
// Config values merge rules:
// 1) config file value overrides default
// 2) command line value overrides any
func loadConfigOrDie() memcached.Config {
	l := log.NewLogger(log.DebugLevel, os.Stderr)
	l.Debug("Memcached server start.\n\n")
	flg := parseFlags()
	//l.Debugf("Flag config: %#v\n", flg)
	if err := validateFlagConf(flg.Config); err != nil {
		l.Fatal(err)
	}
	fileConf := config.Default()

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

	//l.Debugf("File config BEFORE merge: %#v\n", fileConf)
	config.Merge(fileConf, &flg.Config)
	//l.Debugf("File config AFTER merge: %#v\n", fileConf)
	mconf, err := config.Parse(*fileConf)
	if err != nil {
		l.Fatal(err)
	}
	return mconf
}

type Flags struct {
	ConfigPath string
	config.Config
}

// NOTE: without "only stdlib" constraint I would
// github.com/spf13/viper and with custom github.com/mitchellh/mapstructure decode hooks
// for configuration and github.com/spf13/cobra for CLI.
// NOTE: for simplicity configure only from file.
func parseFlags() Flags {
	var f Flags
	flag.StringVar(&f.ConfigPath, "config", "", "path to json config")

	def := config.Default()
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
	flag.StringVar(&f.AOF.Name, "aof-name", "", usage("Append Only File(AOF) name", def.AOF.Name))
	flag.DurationVar(&f.AOF.Sync, "sync", 0, usage("AOF sync period", def.AOF.Sync))
	flag.StringVar(&f.AOF.BufSize, "buf-size", "", usage("AOF buffer size", def.AOF.BufSize))
	flag.BoolVar(&f.AOF.FixCorrupted, "fix-corrupted", false, usage("truncate AOF to valid prefix, if it is possible.", def.AOF.FixCorrupted))
	flag.Parse()
	return f
}

// mergeConfigs overwrite def values with non zero override values
// WARN: not recursive now.

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "%s", usage)
		flag.PrintDefaults()
	}
}

func validateFlagConf(flagConf config.Config) error {
	if flagConf.AOF.Name != "" {
		return nil
	}
	if !util.IsZero(flagConf.AOF) {
		return stackerr.New("Persistence not enabled, but passed some persistence options.\n" +
			"Probably you want pass AOF name.")
	}
	return nil
}
