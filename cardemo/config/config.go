package config

import (
	"github.com/usvc/go-config"
)

var Conf = config.Map{
	"sinks-config": &config.String{
		Default:   "sinks.yaml",
		Usage:     "sink configuration file",
		Shorthand: "f",
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
	"routes-file": &config.String{
		Default:   "./routes.json",
		Usage:     "where the routes file located",
		Shorthand: "r",
	},
}
