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
import time
import uuid
from typing import Any

from datetime import datetime
from multiprocessing import Process, Queue

# External libraries
from kafka.producer import KafkaProducer
from kafka.admin import KafkaAdminClient
from kafka.errors import MessageSizeTooLargeError, NoBrokersAvailable

# Local modules
from ._color import color


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



def is_kafka_alive(
        address_server: str, 
        sasl_mechanism: None=None, 
        sasl_plain_username: str | None=None,
        sasl_plain_password: str | None=None,
    ):
    """
    Check if a Kafka broker is available at the specified address and port.

    Attempts to create a KafkaAdminClient with the given server address and port.
    If a broker is found, returns True. If no brokers are available, logs an error
    and returns False.

    Args:
        address_server (str): The IP address and port of the Kafka broker `<server_ip>:<port>`.

    Returns:
        bool: True if a Kafka broker is available, False otherwise.
    """

    try:
        _ = KafkaAdminClient(
            bootstrap_servers=address_server,
            reconnect_backoff_ms=1_000,
            reconnect_backoff_max_ms=10_000,
            sasl_mechanism=sasl_mechanism,
            sasl_plain_username=sasl_plain_username,
            sasl_plain_password=sasl_plain_password,
        )
        return True  # break when a broker is found

    except NoBrokersAvailable:
        pass
        logging.error(
            color(
                f"{is_kafka_alive.__name__}: Broker not found at {address_server} - Terminating connection...",
                "red",
            )
        )


# TODO: Consider switching to threading to reduce CPU and memory consumption
class ScannerKafkaProducer(Process):
    """
    Kafka Producer used to send data to a Kafka topic.

    Args:
        ip_client (str): Client IP (Local IP, only used for logging info in the IDMEFv2 file)
        addr_server (str): Server address `<server_ip>:<port>`.
        topic (str): The Kafka topic where the message is posted.
        queue_received (Queue): The queue listing the received files (formatted as strings).
        timeout_sec (int): Timeout for the producer.
    """

    def __init__(
        self,
        ip_client: str,
        addr_server: str,
        topic: str,
        queue_received: Queue,
        timeout_sec: int = 10,
        sasl_mechanism: str | None=None,
        sasl_plain_username: str | None=None,
        sasl_plain_password: str | None=None,
    ):

        super().__init__()

        self.ip_client: str = ip_client
        self.addr_server: str = addr_server
        self.topic: str = topic
        self.queue_received: Queue = queue_received
        self.timeout_sec: int = timeout_sec
        self.sasl_mechanism=sasl_mechanism,
        self.sasl_plain_username=sasl_plain_username,
        self.sasl_plain_password=sasl_plain_password,

        self.logger: logging.Logger = logging.getLogger(__class__.__name__)


    def run(self):
        """
        Main loop of the ScannerKafkaProducer.
        """

        self.logger.debug(f"Connecting to Kafka broker at {self.addr_server}...")

        while not is_kafka_alive(self.addr_server):
            self.logger.error(
                color(
                    f"FATAL - Kafka broker not found at {self.addr_server}.",
                    "red",
            )
        )

        while True:

            try:
                data: str = self.queue_received.get_nowait()
            except queue.Empty:
                continue

            try:

                report_data = generate_idmefv2_payload_report(self.ip_client, data)
                producer = KafkaProducer(
                    bootstrap_servers=self.addr_server,
                    compression_type="gzip",
                    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
                    sasl_mechanism=self.sasl_mechanism,
                    sasl_plain_username=self.sasl_plain_username,
                    sasl_plain_password=self.sasl_plain_password,
                )

                future = producer.send(self.topic, report_data)
                result = future.get(timeout=self.timeout_sec)

                self.logger.info(
                    color(f"Message sent successfully to topic '{self.topic}'.", "green")
                )

                # NOTE: Example result content:
                # RecordMetadata(topic='ids', partition=0, topic_partition=TopicPartition(topic='ids', partition=0), offset=836, timestamp=1746533804410, log_start_offset=0, checksum=None, serialized_key_size=-1, serialized_value_size=82265, serialized_header_size=-1)

                self.logger.debug(f"{str(result)}")

            except NoBrokersAvailable:
                self.logger.error(
                    color(f"Broker not found at {self.addr_server}. Retrying...", "yellow")
                )
                time.sleep(5)

            except MessageSizeTooLargeError:
                # TODO: Implement a backup queue to store messages that are too large
                self.logger.error(
                    color(f"FATAL - Message too large.", "red")
                )

            except queue.Full:
                self.logger.error(color(f"Queue is full.", "red"))
                time.sleep(5)
                pass



