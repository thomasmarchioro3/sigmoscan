# SigmoScan

This tool can be used to monitor an interface, collect network packets, and extract flow data every `ROTATE_EVERY_T_SEC` using NFStream and forward the flow data to an Apache Kafka broker.

## Requirements

- Docker 
- (Optional) Docker Compose

## Configuration

SigmoScan can be configured using the `config.ini` file. Use `example_config.ini` as a template.
```sh
cp example_config.ini config.ini
```

Important parameters are:

- `[SCANNER]`
    - `INTERFACE` The network interface to be monitored. You can display available interfaces by running `ip a` on most linux machines.
    - `ROTATE_EVERY_T_SEC` How often (in seconds) a new pcap file is started and the flows of the previous one are sent to Kafka. Must be a whole number of at least 1. Default: `10`.


- `[KAFKA]`
    - `IP_CLIENT` The IP address of this machine. It is only written in the reports sent to Kafka (`Analyzer.IP`).
    - `ADDRESS_SERVER` The address of the Kafka server (e.g., `http://127.0.0.1:9092`)
    - `TOPIC` The Kafka topic where the network flow data should be sent.
    - `SECURITY_PROTOCOL` How SigmoScan connects to Kafka. Default: `PLAINTEXT`.
        - `PLAINTEXT`: no authentication, no encryption.
        - `SASL_PLAINTEXT`: username and password, no encryption.
        - `SASL_SSL`: username and password over TLS. See [SASL_SSL](#sasl_ssl).
    - `SASL_MECHANISM` `PLAIN` or `None`. Use `PLAIN` with `SASL_PLAINTEXT` and `SASL_SSL`.
    - `SASL_PLAIN_USERNAME`, `SASL_PLAIN_PASSWORD` The Kafka username and password. Required when `SASL_MECHANISM=PLAIN`.

SigmoScan logs the config at startup. Passwords are shown as `***` in this log.

### SASL_SSL

With `SECURITY_PROTOCOL=SASL_SSL`, these keys of the `[KAFKA]` section are also used:

| Key | Description | Default |
| --- | --- | --- |
| `SSL_CAFILE` | CA certificate used to check the broker certificate | system CA store |
| `SSL_CERTFILE` | Client certificate. Set it together with `SSL_KEYFILE`. | not set |
| `SSL_KEYFILE` | Client private key. Set it together with `SSL_CERTFILE`. | not set |
| `SSL_PASSWORD` | Password of the client private key, if it has one | not set |
| `SSL_CHECK_HOSTNAME` | `true` or `false`. If `true`, the host in `ADDRESS_SERVER` must match a name in the broker certificate. | `true` |

Put the certificate files in the `certs/` folder. Git ignores this folder, and Docker never copies it into the image. Then use paths relative to `config.ini`:

```ini
[KAFKA]
SECURITY_PROTOCOL=SASL_SSL
SASL_MECHANISM=PLAIN
SASL_PLAIN_USERNAME=<user>
SASL_PLAIN_PASSWORD=<password>
SSL_CAFILE=certs/kafka-ca-1.crt
SSL_CERTFILE=certs/<user>.certificate.pem
SSL_KEYFILE=certs/<user>.key
SSL_PASSWORD=<key password>
```

Notes:

- Relative paths start from the folder of `config.ini`, not from the current directory.
- SigmoScan stops at startup if a file does not exist.
- The SSL keys are ignored with `PLAINTEXT` and `SASL_PLAINTEXT`. In this case SigmoScan logs a warning.
- With Docker, use relative paths inside `certs/`. An absolute path on the host does not exist inside the container.

## Deployment

### With Docker

SigmoScan can be deployed by building the `Dockerfile` in the root directory.

```sh
docker build . -t sigmoscan:latest && docker run -it --rm --network=host --cap-add=NET_ADMIN --cap-add=NET_RAW --cpus=1.0 -v "$(pwd)/certs:/app/certs:ro" sigmoscan:latest
```

The `-v` option mounts the `certs/` folder read-only in the container. It is only needed with `SASL_SSL`.

DISCLAIMER: Since SigmoScan heavily relies on Python multiprocessing, its default resource consumption can be large (2-3 full CPU cores). One can set, e.g., `--cpus=1.0` for `docker run` to limit the number of cores used by SigmoScan to 1.


### With Docker Compose

```sh
docker compose up --build
```

NOTE: The `--build` option can be omitted whenever the `config.ini` file is not changed. Changes in `certs/` do not need a new build, because the folder is mounted at runtime.


## Dev

### Testing

SigmoScan can be tested by running

```
make test
```

