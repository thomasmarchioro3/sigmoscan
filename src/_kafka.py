"""
Copyright (C) 2025, CEA

This program is free software; you can redistribute it and/or modify
it under the terms of the Creative Commons Attribution-NonCommercial-ShareAlike 4.0
International License.

You should have received a copy of the license along with this
program. If not, see <https://creativecommons.org/licenses/by-nc-sa/4.0/>.
"""

import json
import logging
import queue
import uuid
from typing import Any

from datetime import datetime
from multiprocessing import Process, Queue

# External libraries
from kafka.producer import KafkaProducer
from kafka.errors import MessageSizeTooLargeError

# Local modules
from ._color import color
from ._config import KafkaConfig


def generate_idmefv2_payload_report(ip_client: str, data: str) -> dict[str, Any]:
    """
    Args:
        ip_client (str): Client IP (Local IP, only used for logging info in the IDMEFv2 file)
        data (str): Data to be reported
    """

    report_id = str(uuid.uuid4())

    # use ISO 8601 for time
    curr_time = datetime.now().isoformat()

    report = {
        "Version": "2.D.V03",
        "ID": report_id,
        "CreateTime": curr_time,
        "Analyzer": {
            "IP": ip_client,
        },
        "Attachment": {
            "Name": "Data",  # TODO: Add name option
            "ContentType": "text/plain",
            "Content": data,
        },
    }

    return report


# TODO: Consider switching to threading to reduce CPU and memory consumption
class ScannerKafkaProducer(Process):
    """
    Kafka Producer used to send data to a Kafka topic.

    Args:
        ip_client (str): Client IP (Local IP, only used for logging info in the IDMEFv2 file)
        kafka_config (KafkaConfig): Server address, topic, and SASL/SSL
            settings, as returned by `parse_config`.
        queue_received (Queue): The queue listing the received files (formatted as strings).
        timeout_sec (int): Timeout for the producer.
    """

    def __init__(
        self,
        ip_client: str,
        kafka_config: KafkaConfig,
        queue_received: Queue,
        timeout_sec: int = 10,
    ):

        super().__init__()

        self.ip_client: str = ip_client
        self.kafka_config: KafkaConfig = kafka_config
        self.topic: str = kafka_config["topic"]
        self.queue_received: Queue = queue_received
        self.timeout_sec: int = timeout_sec

        self.logger: logging.Logger = logging.getLogger(__class__.__name__)


    def _create_producer(self) -> KafkaProducer:
        cfg = self.kafka_config
        return KafkaProducer(
            bootstrap_servers=cfg["addr_server"],
            security_protocol=cfg["security_protocol"],
            value_serializer=lambda m: json.dumps(m).encode('utf-8'),
            sasl_mechanism=cfg["sasl_mechanism"],
            sasl_plain_username=cfg["sasl_plain_username"],
            sasl_plain_password=cfg["sasl_plain_password"],
            # All None (and check_hostname True) unless the protocol is SASL_SSL,
            # which are the KafkaProducer defaults.
            ssl_cafile=cfg["ssl_cafile"],
            ssl_certfile=cfg["ssl_certfile"],
            ssl_keyfile=cfg["ssl_keyfile"],
            ssl_password=cfg["ssl_password"],
            ssl_check_hostname=cfg["ssl_check_hostname"],
        )

    def run(self):
        """
        Main loop of the ScannerKafkaProducer.

        One KafkaProducer is created and reused for all messages. It reconnects
        by itself if the broker goes down and comes back. If creating it fails
        (e.g. the broker is unreachable at startup), creation is tried again
        with the next message.

        A timeout on `future.get` only means the message was not confirmed in
        `timeout_sec`. KafkaProducer keeps trying to deliver it in the
        background until its `delivery_timeout_ms` (default 120 s) runs out.
        """

        producer: KafkaProducer | None = None

        while True:

            try:
                # Block until data arrives, instead of polling without pause
                data: str = self.queue_received.get(timeout=1)
            except queue.Empty:
                continue

            try:

                report_data = generate_idmefv2_payload_report(self.ip_client, data)

                if producer is None:
                    producer = self._create_producer()

                future = producer.send(self.topic, report_data)
                if future is None:
                    self.logger.error(color(f"Impossible to send message to kafka topic '{self.topic}'", "red"))
                    continue

                result = future.get(timeout=self.timeout_sec)

                self.logger.info(
                    color(f"Message sent successfully to topic '{self.topic}'.", "green")
                )

                # NOTE: Example result content:
                # RecordMetadata(topic='ids', partition=0, topic_partition=TopicPartition(topic='ids', partition=0), offset=836, timestamp=1746533804410, log_start_offset=0, checksum=None, serialized_key_size=-1, serialized_value_size=82265, serialized_header_size=-1)

                self.logger.debug(f"{str(result)}")


            except MessageSizeTooLargeError:
                # The connection is fine, so the producer is kept
                # TODO: Implement a backup queue to store messages that are too large
                self.logger.error(
                    color(f"FATAL - Message too large.", "red")
                )

            except Exception as e:
                # The producer is kept: closing it fails while the broker is
                # down, and it reconnects by itself when the broker is back.
                self.logger.error(color(f"{e}", "red"))


