# Test Kafka broker with SASL_SSL

A single-node Kafka broker (`apache/kafka:4.0.0`, KRaft) for testing SigmoScan with `SECURITY_PROTOCOL=SASL_SSL`. It uses the same connection setup as the `example-new-kafka/` clients:

- TLS, with a broker certificate signed by a test CA.
- A client certificate is **required** (the client key is encrypted with a password).
- SASL `PLAIN` with a username and a password.

**Warning:** all certificates, keys and passwords in this folder are public test values. Never use them for a real broker.

```
.
├── Dockerfile
├── docker-compose.yml
├── server.properties    broker config (listeners, SASL users, TLS)
├── start.sh             formats the storage on the first start, then starts Kafka
├── gen-certs.sh         makes the files in secrets/ and sigmoscan/
├── secrets/             broker files: CA certificate, broker keystore
├── sigmoscan/           client files: CA certificate, client certificate, client key
└── client.env           settings for the example-new-kafka/ clients
```

## Credentials

| What | Value |
| --- | --- |
| Bootstrap address | `127.0.0.1:9092` (or `localhost:9092`) |
| SASL username / password | `sigmoscan` / `sigmoscan-secret` |
| CA certificate | `sigmoscan/kafka-ca-1.crt` |
| Client certificate | `sigmoscan/sigmoscan.certificate.pem` |
| Client key / key password | `sigmoscan/sigmoscan.key` / `sigmoscan-key-password` |

The broker certificate is valid for `localhost`, `127.0.0.1` and `kafka-sasl-ssl`, so clients can keep hostname checking on.

## Start and stop

```sh
cd testutils/kafka-sasl-ssl
docker compose up --build -d
```

The container is ready when `docker ps` shows it as `healthy` (about 10 seconds).

```sh
docker compose down
```

Notes:

- The broker uses port `9092`, like the `kafkacontainer` broker in `testutils/kafka/`. Stop one before starting the other.
- Topics are created when a client first writes to them.
- Messages are lost when the container is removed (`docker compose down`).

## Use it with SigmoScan

In `config.ini` at the root of the repository:

```ini
[KAFKA]
IP_CLIENT=127.0.0.1
ADDRESS_SERVER=127.0.0.1:9092
TOPIC=network
SECURITY_PROTOCOL=SASL_SSL
SASL_MECHANISM=PLAIN
SASL_PLAIN_USERNAME=sigmoscan
SASL_PLAIN_PASSWORD=sigmoscan-secret
SSL_CAFILE=testutils/kafka-sasl-ssl/sigmoscan/kafka-ca-1.crt
SSL_CERTFILE=testutils/kafka-sasl-ssl/sigmoscan/sigmoscan.certificate.pem
SSL_KEYFILE=testutils/kafka-sasl-ssl/sigmoscan/sigmoscan.key
SSL_PASSWORD=sigmoscan-key-password
```

These paths also work when SigmoScan runs in Docker, because the SigmoScan image contains `testutils/`.

Then run `make run`. Expected log line, once per `ROTATE_EVERY_T_SEC`:

```
Message sent successfully to topic 'network'.
```

## Use it with the example-new-kafka/ clients

`client.env` has the settings for this broker. Load it in the shell before running the clients. Environment variables take priority over `example-new-kafka/.env`, so you do not need to change that file.

```sh
cd example-new-kafka
set -a; . ../testutils/kafka-sasl-ssl/client.env; set +a
python kafka_producer.py
python kafka_consumer.py
```

Expected output of `kafka_producer.py`:

```
OK: network[0] offset=<n>
```

## Read the messages from inside the container

The broker also has a plaintext listener on `localhost:29092`, which is only reachable from inside the container. The Kafka CLI tools can use it without credentials:

```sh
docker exec -it kafka-sasl-ssl /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:29092 --topic network --from-beginning
```

## Make new certificates

The certificates are valid for 10 years. To make new ones (for example, to add a name to the broker certificate in `BROKER_SAN`):

```sh
./gen-certs.sh
docker compose up --build -d
```

`gen-certs.sh` needs `openssl` and overwrites the files in `secrets/` and `sigmoscan/`. It does not keep the CA key, so it always makes a new CA together with the new certificates.

## What a wrong config looks like

| Client config | Broker log | SigmoScan log |
| --- | --- | --- |
| Wrong SASL password | `Authentication failed: Invalid username or password` | `KafkaTimeoutError: Unable to bootstrap from 127.0.0.1:9092` |
| No client certificate | `SSL handshake failed` | same |
| Wrong key password | nothing (the client fails before it connects) | same |

SigmoScan logs this error about 30 seconds after the first message, because the producer tries to connect for that long before it gives up. The log does not say which of the three problems it is: check the broker log with `docker logs kafka-sasl-ssl`.
