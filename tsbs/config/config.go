package config

import (
	"github.com/usvc/go-config"
)

var Conf = config.Map{
	"source": &config.String{
		Default:   "./data/devops-data-devops.gz",
		Usage:     "tsbs data file",
		Shorthand: "f",
	},
	"timeplus-address": &config.String{
		Default:   "http://localhost:8000",
		Usage:     "the server address of timeplus",
		Shorthand: "a",
	},
	"timeplus-apikey": &config.String{
		Default:   "",
		Usage:     "the apikey of timeplus",
		Shorthand: "k",
	},
	"metrics-schema": &config.String{
		Default: "single",
		Usage:   "the metrics store schema, default to single, support single|multiple",
	},
	"skip-create-streams": &config.Bool{
		Default: false,
		Usage:   "whether to skip stream creation",
	},
	"http-timeout": &config.Int{
		Default: 10,
		Usage:   "HTTP timeout in seconds, default to 10",
	},
	"http-max-connection-per-host": &config.Int{
		Default: 100,
		Usage:   "HTTP max connection per host, default to 100",
	},
	"http-max-idle-connection": &config.Int{
		Default: 100,
		Usage:   "HTTP max idle connection, default to 100",
	},
	"http-max-idle-connection-per-host": &config.Int{
		Default: 100,
		Usage:   "HTTP max idle connection per host, default to 100",
	},
	"log-level": &config.String{
		Default: "info",
		Usage:   "level of log, support panic|fatal|error|warn|info|debug|trace",
	},
}
