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
├── client.env           settings for the example-new-kafka/ clients
└── sigmoscan.env        settings for SigmoScan (use with --envfile or docker run --env-file)
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

`sigmoscan.env` has the connection settings for this broker. In `config.ini` at the root of the repository, set:

```ini
[KAFKA]
SECURITY_PROTOCOL=SASL_SSL
SASL_MECHANISM=PLAIN
```

Then start SigmoScan with `sigmoscan.env` instead of `.env`:

```sh
uv run main.py --envfile testutils/kafka-sasl-ssl/sigmoscan.env
```

The certificate paths in `sigmoscan.env` start from the folder of `config.ini`. They also work when SigmoScan runs in Docker (`docker run --env-file testutils/kafka-sasl-ssl/sigmoscan.env ...`), because the SigmoScan image contains `testutils/`.

Expected log line, once per `ROTATE_EVERY_T_SEC`:

```
Message sent successfully to topic 'network'.
```

## End-to-end test

This test starts the broker, starts SigmoScan in Docker, and checks that SigmoScan's reports arrive in the topic. Run all commands from the root of the repository.

You need:

- A `config.ini` at the root of the repository, because the SigmoScan image build reads it. If you do not have one: `cp example_config.ini config.ini`.
- `TEMP_PCAP_PATH=/tmp/pcap` in that `config.ini`. The build gives this folder to the `tcpdump` user, and the test config below uses the same folder.

### 1. Start a fresh broker

`docker compose down` removes the old container, so the topic has no messages from earlier tests.

```sh
cd testutils/kafka-sasl-ssl
docker compose down
docker compose up --build -d
until [ "$(docker inspect -f '{{.State.Health.Status}}' kafka-sasl-ssl)" = healthy ]; do sleep 2; done; echo "broker ready"
cd ../..
```

Last line: `broker ready`

### 2. Write the SigmoScan config for the test

This writes a separate config file, so your `config.ini` does not change. Step 3 mounts it as `/app/config.ini` in the container. The connection settings come from `sigmoscan.env`.

```sh
cat > testutils/kafka-sasl-ssl/e2e_config.ini <<'EOF'
[SCANNER]
; Loopback: SigmoScan captures its own traffic to the broker, so each message has flows
INTERFACE=lo
TEMP_PCAP_PATH=/tmp/pcap
ROTATE_EVERY_T_SEC=5

[KAFKA]
IP_CLIENT=10.99.99.99
SECURITY_PROTOCOL=SASL_SSL
SASL_MECHANISM=PLAIN
EOF
```

`IP_CLIENT=10.99.99.99` is only a marker: step 4 uses it to recognize the reports of this test.

### 3. Build and start SigmoScan

The build copies `testutils/kafka-sasl-ssl/` into the image, so the certificate paths in `sigmoscan.env` exist in the container. The `sigmoscan:e2e` tag keeps your `sigmoscan:latest` image unchanged.

```sh
docker build . -t sigmoscan:e2e
docker run -d --name sigmoscan-e2e --network=host --cap-add=NET_ADMIN --cap-add=NET_RAW --cpus=1.0 \
    --env-file testutils/kafka-sasl-ssl/sigmoscan.env \
    -v "$(pwd)/testutils/kafka-sasl-ssl/e2e_config.ini:/app/config.ini:ro" \
    sigmoscan:e2e
docker logs -f sigmoscan-e2e
```

Expected log line, every 5 seconds:

```
Message sent successfully to topic 'network'.
```

Press Ctrl+C to stop following the log. The container keeps running.

### 4. Check the reports in the topic

This command reads 3 messages from the topic. For each message, it prints the creation time, `IP_CLIENT` and the number of flows in the CSV:

```sh
docker exec kafka-sasl-ssl /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:29092 --topic network --from-beginning \
    --max-messages 3 --timeout-ms 30000 2>/dev/null \
| python3 -c '
import sys, json
for line in sys.stdin:
    r = json.loads(line)
    n = len(r["Attachment"]["Content"].splitlines()) - 1
    print(r["CreateTime"], r["Analyzer"]["IP"], f"{n} flows")
'
```

Expected output: 3 lines like this one, with at least 1 flow each:

```
2026-11-11T14:05:10.123456 10.99.99.99 4 flows
```

### 5. Clean up

```sh
docker rm -f sigmoscan-e2e
(cd testutils/kafka-sasl-ssl && docker compose down)
rm testutils/kafka-sasl-ssl/e2e_config.ini
```

### If the test fails

- No `Message sent successfully` line: read the errors in `docker logs sigmoscan-e2e` and in `docker logs kafka-sasl-ssl`. See also [What a wrong config looks like](#what-a-wrong-config-looks-like).
- `Permission denied` on `/tmp/pcap/...`: `TEMP_PCAP_PATH` in your `config.ini` is not `/tmp/pcap`.
- `docker exec` prints nothing after 30 seconds: SigmoScan did not send any message. Check step 3.

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

| Client config | Broker log | SigmoScan log (from kafka-python) |
| --- | --- | --- |
| Wrong SASL password | `Authentication failed: Invalid username or password` | `Connection lost: KafkaConnectionError: socket disconnected` |
| No client certificate | `SSL handshake failed` | `[SSL: SSLV3_ALERT_BAD_CERTIFICATE] ssl/tls alert bad certificate`, with a traceback |
| Wrong key password | nothing (the client fails before it connects) | `Connection failed: [SSL] PEM lib` |

These lines repeat at each reconnect attempt. About 30 seconds after the first message, SigmoScan also logs `KafkaTimeoutError: Unable to bootstrap from 127.0.0.1:9092`.

kafka-python does not report the reason for a wrong SASL password: it only sees that the broker closes the connection. Check the broker log with `docker logs kafka-sasl-ssl`.
