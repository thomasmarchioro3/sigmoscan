"""
Copyright (C) 2025, CEA

This program is free software; you can redistribute it and/or modify
it under the terms of the Creative Commons Attribution-NonCommercial-ShareAlike 4.0
International License.

You should have received a copy of the license along with this
program. If not, see <https://creativecommons.org/licenses/by-nc-sa/4.0/>.
"""

import io
import json
import logging
import queue
import time
from datetime import datetime

from multiprocessing import Process, Queue

import pandas as pd
from pandas.io.pytables import DataCol
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient
from kafka.errors import MessageSizeTooLargeError, NoBrokersAvailable

from ._color import color


def is_kafka_alive(address_server: str):
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
        KafkaAdminClient(
            bootstrap_servers=address_server,
            reconnect_backoff_ms=1_000,
            reconnect_backoff_max_ms=10_000,
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

    return False


class EngineKafkaConsumer(Process):
    """
    Kafka consumer process.
    
    Args:
        addr_server (str): The IP address and port of the Kafka broker `<server_ip>:<port>`.
        topic (str): The Kafka topic to consume from.
        queue_ingested (Queue): The queue to store ingested messages.
    """

    def __init__(
        self,
        addr_server: str,
        topic: str,
        queue_ingested: Queue,
    ):
        super().__init__()

        self.addr_server: str = addr_server
        self.topic: str = topic
        self.queue_ingested: Queue = queue_ingested

        self.logger: logging.Logger = logging.getLogger(__class__.__name__)


    def run(self) -> None:
        """
        Main loop of the consumer process.
        """

        self.logger.debug(f"Connecting to Kafka broker at {self.addr_server}...")

        consumer = None

        while consumer is None:

            try:
                consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=self.addr_server,
                    auto_offset_reset="earliest",
                    enable_auto_commit=True,
                    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                )
            except NoBrokersAvailable:
                logging.error(
                    f"{self.__class__} Broker not found at {self.address_server}. Retrying in 5 seconds..."
                )
                time.sleep(5)
        
        while True:

            for message in consumer:
                try:
                    message_ts: str = message.value["CreateTime"]
                    data_str: str = message.value["Attachment"]["Content"]
                    df = pd.read_csv(io.StringIO(data_str))

                    # TODO Define ingested messages as TypedDict and add validation
                    ingested_message = {
                        "timestamp": message_ts,
                        "df": df,
                    }
                    self.queue_ingested.put(ingested_message)
                    self.logger.info(color(f"Message successfully ingested from topic '{self.topic}'.", "green"))
                except queue.Full:
                    self.logger.warning(color("Queue is full. Message will be dropped.", "yellow"))

                except Exception as e:
                    self.logger.error(color(f"Error ingesting message: {e.__class__.__name__}:{e}", "red"))

