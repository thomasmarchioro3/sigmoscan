"""
Copyright (C) 2025, CEA

This program is free software; you can redistribute it and/or modify
it under the terms of the Creative Commons Attribution-NonCommercial-ShareAlike 4.0
International License.

You should have received a copy of the license along with this
program. If not, see <https://creativecommons.org/licenses/by-nc-sa/4.0/>.
"""

import argparse
import logging
import os
import time
from multiprocessing import Queue

import pandas as pd

from src.capture import capture_live_interface
from src.color import color
from src.config import mask_secrets, parse_config
from src.flow import get_df_from_pcap
from src.kafka import ScannerKafkaProducer
from src.logo import ASCII_ART
from src.network import check_existing_network_interface
from src.osutils import TempPcapHandler


DEFAULT_CONFIG_FILE: str = "config.ini"

# Kafka connection settings (address, topic, credentials, certificates)
DEFAULT_ENV_FILE: str = ".env"

# How long to wait before checking again when no pcap file is ready
POLL_INTERVAL_SEC: float = 0.5

# Log level of kafka-python. WARNING shows the cause of connection errors
# (broker unreachable, TLS errors, wrong key password).
# Never set it to DEBUG, also not with --debug: at DEBUG, kafka-python logs
# the SASL password in plain text.
KAFKA_LOG_LEVEL: int = logging.WARNING


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--configfile", type=str, default=DEFAULT_CONFIG_FILE, help="Path to config file")
    parser.add_argument(
        "--envfile", type=str, default=DEFAULT_ENV_FILE,
        help="Path to the .env file with the Kafka connection settings. Environment variables with the same names take priority.",
    )
    parser.add_argument("--savelog", action="store_true", help="If used, stores logs in file in `scanner_<timestamp_ns>.log`")
    parser.add_argument("--debug", action="store_true", help="If used, sets the log level to DEBUG.")
    args = parser.parse_args()

    configfile: str = args.configfile
    if not os.path.isfile(configfile):
        error_msg= f"Config file {configfile} not found."
        raise ValueError(error_msg)

    savelogs: bool = args.savelog
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    
    if savelogs:
        handlers.append(
            logging.FileHandler(f"scanner_{time.time_ns()}.log", "w")  
        )
    logging.basicConfig(
        level=logging.INFO if not args.debug else logging.DEBUG,
        handlers=handlers,
        format = r"[%(asctime)s][%(name)s][%(levelname)s] %(message)s",
    )

    logging.getLogger("kafka").setLevel(KAFKA_LOG_LEVEL)
    config = parse_config(configfile, args.envfile)

    print(ASCII_ART)
    logging.info(f"Config: {mask_secrets(config)}")

    temp_dir = config["temp_pcap_path"]
    rotate_every_t_seconds = config["rotate_every_t_sec"]
    temp_pcap_handler = TempPcapHandler(temp_dir, rotate_every_t_seconds)
    temp_pcap_handler.clean_pcap_files()

    # Initialize queues
    queue_nfstream: Queue = Queue()

    if config["interface"] is not None and not check_existing_network_interface(config["interface"]):
        logging.error(color(f"Fatal: Interface {config['interface']} does not exist.", "red"))


     # Start processes
    tcpdump_process = capture_live_interface(
        interface=config["interface"],
        rotate_every_t_sec=rotate_every_t_seconds,
        temp_dir=temp_dir,
    )

    kafka_producer = ScannerKafkaProducer(
        ip_client=str(config["ip_client"]),
        kafka_config=config["kafka_config"],
        queue_received=queue_nfstream,
        timeout_sec=10,  # TODO: Move this parameter to config
    )

    kafka_producer.start()

    try:
        while True:
            pcap_file = temp_pcap_handler.get_next_pcap_file()
            if pcap_file is None:
                # check that tcpdump process is still alive
                if tcpdump_process.poll() is not None:
                    if tcpdump_process.stderr is not None:
                        err = tcpdump_process.stderr.read().decode()
                        err = err.rstrip('\n')
                        logging.error(color(f"{err}", "red"))
                        raise KeyboardInterrupt

                time.sleep(POLL_INTERVAL_SEC)
                continue

            df = get_df_from_pcap(pcap_file)

            if not isinstance(df, pd.DataFrame):
                logging.error(f"Failed to process pcap: {pcap_file}. Returned data is not a pandas DataFrame, but {type(df)}.")
                continue

            data = df.to_csv(index=False)
            queue_nfstream.put(data)
            logging.debug(f"Deleting temp pcap: {pcap_file}")

            temp_pcap_handler.remove_pcap_file(pcap_file)



    except KeyboardInterrupt:
        if tcpdump_process:
            logging.info("Stopping tcpdump process")
            tcpdump_process.terminate()
        if kafka_producer and kafka_producer.is_alive():
            logging.info("Stopping kafka producer process")
            kafka_producer.terminate()


if __name__ == "__main__":
    main()
