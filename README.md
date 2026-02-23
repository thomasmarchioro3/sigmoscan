# SigmoScan

This tool can be used to monitor an interface, collect network packets, and extract flow data every `ROTATE_EVERY_T_SEC` using NFStream and forward the flow data to an Apache Kafka broker.

## Requirements

- Docker 

## Configuration

SigmoScan can be configured using the `config.ini` file.
Important parameters are:

- `[SCANNER]`
    - `INTERFACE` The network interface to be monitored. You can display available interfaces by running `ip a` on most linux machines.


- `[KAFKA]`
    - `ADDRESS_SERVER` The address of the Kafka server (e.g., `http://127.0.0.1:9092`)
    - `TOPIC` The Kafka topic where the network flow data should be sent.

## Deployment

SigmoScan can be deployed by building the `Dockerfile` in the root directory.

```sh
docker build . -t sigmoscan:latest && docker run -it --rm --network=host --cap-add=NET_ADMIN --cap-add=NET_RAW --cpus=1.0 sigmoscan:latest
```

DISCLAIMER: Since SigmoScan heavily relies on Python multiprocessing, its default resource consumption can be large (2-3 full CPU cores). One can set, e.g., `--cpus=1.0` for `docker run` to limit the number of cores used by SigmoScan to 1.

## Dev

### Testing

SigmoScan can be tested by running

```
make test
```

