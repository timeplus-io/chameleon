package config

import (
	"github.com/usvc/go-config"
)

var Conf = config.Map{
	"server-addr": &config.String{
		Default:   "0.0.0.0",
		Usage:     "ip interface the server should listen on",
		Shorthand: "a",
	},
}
