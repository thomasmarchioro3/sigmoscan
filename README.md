# SigmoScan

This tool can be used to monitor an interface, collect network packets, and extract flow data every `ROTATE_EVERY_T_SEC` using NFStream and forward the flow data to an Apache Kafka broker.

## Requirements

- Docker 
- (Optional) Docker Compose

## Configuration

SigmoScan reads two files:

- `config.ini`: the scanner settings and how to connect to Kafka (protocol, SASL mechanism). Use `example_config.ini` as a template.
- `.env`: the Kafka connection settings, including the secrets (address, topic, credentials, certificates). Use `example.env` as a template. The variable names are the same as in `example-new-kafka/`.

```sh
cp example_config.ini config.ini
cp example.env .env
```

Git ignores `config.ini` and `.env`, and Docker never copies `.env` into the image.

### config.ini

- `[SCANNER]`
    - `INTERFACE` The network interface to be monitored. You can display available interfaces by running `ip a` on most linux machines.
    - `ROTATE_EVERY_T_SEC` How often (in seconds) a new pcap file is started and the flows of the previous one are sent to Kafka. Must be a whole number of at least 1. Default: `10`.

- `[KAFKA]`
    - `IP_CLIENT` The IP address of this machine. It is only written in the reports sent to Kafka (`Analyzer.IP`).
    - `SECURITY_PROTOCOL` How SigmoScan connects to Kafka. Default: `PLAINTEXT`.
        - `PLAINTEXT`: no authentication, no encryption.
        - `SASL_PLAINTEXT`: username and password, no encryption.
        - `SASL_SSL`: username and password over TLS. See [SASL_SSL](#sasl_ssl).
    - `SASL_MECHANISM` `PLAIN` or `None`. Use `PLAIN` with `SASL_PLAINTEXT` and `SASL_SSL`.
    - `SSL_CHECK_HOSTNAME` `true` or `false`, only used with `SASL_SSL`. If `true`, the host in `KAFKA_BOOTSTRAP_SERVERS` must match a name in the broker certificate. Default: `true`.

### .env

| Variable | Description | Required |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | Address of the Kafka server, e.g. `127.0.0.1:9092`. Several servers are separated by commas. | yes |
| `KAFKA_TOPIC` | The Kafka topic where the network flow data is sent | yes |
| `KAFKA_SASL_USERNAME` | Kafka username | with `SASL_MECHANISM=PLAIN` |
| `KAFKA_SASL_PASSWORD` | Kafka password | with `SASL_MECHANISM=PLAIN` |
| `KAFKA_CA_FILE` | CA certificate used to check the broker certificate. Default: system CA store. | no, `SASL_SSL` only |
| `KAFKA_CERT_FILE` | Client certificate. Set it together with `KAFKA_KEY_FILE`. | no, `SASL_SSL` only |
| `KAFKA_KEY_FILE` | Client private key. Set it together with `KAFKA_CERT_FILE`. | no, `SASL_SSL` only |
| `KAFKA_KEY_PASSWORD` | Password of the client private key, if it has one | no, `SASL_SSL` only |

Notes:

- An environment variable with the same name takes priority over the value in `.env`.
- An empty value means "not set".
- Use another file with `--envfile <path>`. Default: `.env` in the current directory.
- Older versions of SigmoScan read these settings from the `[KAFKA]` section of `config.ini` (`ADDRESS_SERVER`, `TOPIC`, `SASL_PLAIN_USERNAME`, `SASL_PLAIN_PASSWORD`, `SSL_CAFILE`, `SSL_CERTFILE`, `SSL_KEYFILE`, `SSL_PASSWORD`). SigmoScan stops at startup if `config.ini` still has one of them, and the error names the variable that replaces it.

SigmoScan logs the config at startup. Passwords are shown as `***` in this log.

### SASL_SSL

Put the certificate files in the `certs/` folder. Git ignores this folder, and Docker never copies it into the image. Then use paths relative to `config.ini`:

`config.ini`:

```ini
[KAFKA]
SECURITY_PROTOCOL=SASL_SSL
SASL_MECHANISM=PLAIN
```

`.env`:

```sh
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
KAFKA_TOPIC=network
KAFKA_SASL_USERNAME=<user>
KAFKA_SASL_PASSWORD=<password>
KAFKA_CA_FILE=certs/kafka-ca-1.crt
KAFKA_CERT_FILE=certs/<user>.certificate.pem
KAFKA_KEY_FILE=certs/<user>.key
KAFKA_KEY_PASSWORD=<key password>
```

Notes:

- Relative paths start from the folder of `config.ini` (not of `.env`, and not the current directory).
- SigmoScan stops at startup if a file does not exist.
- The SSL variables are ignored with `PLAINTEXT` and `SASL_PLAINTEXT`. In this case SigmoScan logs a warning.
- With Docker, use relative paths inside `certs/`. An absolute path on the host does not exist inside the container.

## Deployment

### With Docker

SigmoScan can be deployed by building the `Dockerfile` in the root directory.

```sh
docker build . -t sigmoscan:latest && docker run -it --rm --network=host --cap-add=NET_ADMIN --cap-add=NET_RAW --cpus=1.0 --env-file .env -v "$(pwd)/certs:/app/certs:ro" sigmoscan:latest
```

- `--env-file .env` passes the Kafka connection settings as environment variables. **Do not put quotes around the values in `.env`:** `docker run --env-file` keeps them as part of the value.
- `-v` mounts the `certs/` folder read-only in the container. It is only needed with `SASL_SSL`.

DISCLAIMER: Since SigmoScan heavily relies on Python multiprocessing, its default resource consumption can be large (2-3 full CPU cores). One can set, e.g., `--cpus=1.0` for `docker run` to limit the number of cores used by SigmoScan to 1.


### With Docker Compose

```sh
docker compose up --build
```

Docker Compose reads `.env` and mounts `certs/` by itself (see `docker-compose.yml`).

NOTE: The `--build` option can be omitted whenever the `config.ini` file is not changed. Changes in `.env` and `certs/` do not need a new build, because they are passed at runtime.


## Dev

### Testing

SigmoScan can be tested by running

```
make test
```

