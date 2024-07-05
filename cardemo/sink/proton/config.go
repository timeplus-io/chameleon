package proton

import (
	"github.com/spf13/viper"
)

type config struct {
	Host            string `json:"host,omitempty"`
	Port            int    `json:"port,omitempty"`
	User            string `json:"user,omitempty"`
	Password        string `json:"password,omitempty"`
	Debug           bool   `json:"debug,omitempty"`
	FloatFormat     string `json:"float_format,omitempty"`
	TimeFormat      string `json:"time_format,omitempty"`
	MaxIdleConns    int    `json:"max_idle_conns,omitempty"`
	ConnMaxLifetime int    `json:"conn_max_lifetime,omitempty"`
	ConnMaxIdleTime int    `json:"conn_max_idle_time,omitempty"`
}

func NewConfig(host, user, password string, port int) config {
	return config{
		Host:            host,
		Port:            port,
		User:            user,
		Password:        password,
		MaxIdleConns:    viper.GetInt("proton-max-idle-conns"),
		ConnMaxLifetime: viper.GetInt("proton-conn-max-lifetime"),
		ConnMaxIdleTime: viper.GetInt("proton-conn-max-idle-time"),
	}
}
