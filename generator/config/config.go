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
	"server-port": &config.Int{
		Default:   8000,
		Usage:     "port the server should listen on",
		Shorthand: "p",
	},
	"log-level": &config.String{
		Default: "info",
		Usage:   "level of log, support panic|fatal|error|warn|info|debug|trace",
	},
	"log-format": &config.String{
		Default: "text",
		Usage:   "format of log, support json|text",
	},
	"log-file-path": &config.String{
		Default: "neutron.log",
		Usage:   "log file path, default to /tmp/neutron.log. panic will be log to a separated .panic file under the same folder",
	},
}
