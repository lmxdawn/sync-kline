package config

import (
	"github.com/jinzhu/configor"
)

type AppConfig struct {
	Port uint `yaml:"port"`
}

type MongoConfig struct {
	Uri string `yaml:"uri"`
}

type EngineConfig struct {
	Platform string   `yaml:"platform"`  // 平台
	ProxyUrl string   `yaml:"proxy_url"` // 代理
	WsUrl    string   `yaml:"ws_url"`    // ws链接
	HttpUrl  string   `yaml:"http_url"`  // http链接
	Symbols  []string `yaml:"symbols"`   // 交易对
	Periods  []string `yaml:"periods"`   // 交易对
}

type Config struct {
	App     AppConfig
	Mongo   MongoConfig
	Engines []EngineConfig
}

func NewConfig(confPath string) (Config, error) {
	var config Config
	if confPath != "" {
		err := configor.Load(&config, confPath)
		if err != nil {
			return config, err
		}
	} else {
		err := configor.Load(&config, "config/config-example.yml")
		if err != nil {
			return config, err
		}
	}
	return config, nil
}
