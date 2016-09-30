package memcached

type Config struct {
	Port           int
	IP             string
	MaxConnections int
	CacheSize      int
	CacheDir       string
	LogFile        string
	LogLevel       string
}
