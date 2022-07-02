
# Timeplus TSBS data loader

this tool helps to load tsbs benchmark dataset into timeplus

## quick stark

to load devops dataset, run :

```sh
docker run timeplus/tsbsloader:latest -a <timeplus_address> -k <timeplus_apikey>
```

## detail options

run `docker run timeplus/tsbsloader:latest -h` will show all options

```
Usage:
  tsbs [flags]

Flags:
      --config string                           config file (default is $HOME/.tsbs.yaml)
  -h, --help                                    help for tsbs
      --http-max-connection-per-host int        HTTP max connection per host, default to 100 (default 100)
      --http-max-idle-connection int            HTTP max idle connection, default to 100 (default 100)
      --http-max-idle-connection-per-host int   HTTP max idle connection per host, default to 100 (default 100)
      --http-timeout int                        HTTP timeout in seconds, default to 10 (default 10)
      --log-level string                        level of log, support panic|fatal|error|warn|info|debug|trace (default "info")
      --metrics-schema string                   the metrics store schema, default to single, support single|multiple (default "single")
      --skip-create-streams                     whether to skip stream creation
  -f, --source string                           tsbs data file (default "./data/devops-data-devops.gz")
  -a, --timeplus-address string                 the server address of timeplus (default "http://localhost:8000")
  -k, --timeplus-apikey string                  the apikey of timeplus
```


