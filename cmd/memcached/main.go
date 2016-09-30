package main

import (
	"flag"
	"os"

	"github.com/Skipor/memcached/log"
)

type Flags struct {
	configPath string
}

// NOTE: without "only stdlib" constraint I would
// github.com/spf13/viper for configuration and github.com/spf13/cobra for CLI.
// NOTE: for simplicity configure only from file.
func ParseFlags() Flags {
	var f Flags
	flag.StringVar(&f.configPath, "config", "./config.json", "path to json config")
	flag.Parse()
	return f
}

func main() {
	l := log.NewLogger(log.DebugLevel, os.Stdout)
	l.Debug("Hello ", " world!")
}
