package main

type Config struct {
	Port        int    `json:"port"`
	IP          string `json:"ip"`
	CacheSize   int    `json:"cache-size"`
	CacheDir    string `json:"cache-dir"`
	LogFile     string `json:"log-file"`
	LogLevel    string `json:"log-level"`
	MaxItemSize string `json:"max-item-size"`
}
