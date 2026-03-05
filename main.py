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
from src.config import parse_config
from src.flow import get_df_from_pcap
from src.kafka import ScannerKafkaProducer
from src.logo import ASCII_ART
from src.network import check_existing_network_interface
from src.osutils import TempPcapHandler


DEFAULT_CONFIG_FILE: str = "config.ini"


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--configfile", type=str, default=DEFAULT_CONFIG_FILE, help="Path to config file")
    parser.add_argument("--savelog", action="store_true", help="If used, stores logs in file in `scanner_<timestamp_ns>.log`")
    parser.add_argument("--debug", action="store_true", help="If used, sets the log level to DEBUG.")
    args = parser.parse_args()

    configfile: str = args.configfile
    if not os.path.isfile(configfile):
        error_msg= f"Config file {configfile} not found."
        raise ValueError(error_msg)

    rotate_every_t_seconds: float = 10

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

    # disable Kafka logs
    logging.getLogger("kafka").setLevel(1000)
    config = parse_config(configfile)

    print(ASCII_ART)
    logging.info(f"Config: {config}")

    temp_dir = config["temp_pcap_path"]
    temp_pcap_handler = TempPcapHandler(temp_dir, rotate_every_t_seconds)
    temp_pcap_handler.clean_pcap_files()

    # Initialize queues
    queue_nfstream: Queue = Queue()

    if config["interface"] is not None and not check_existing_network_interface(config["interface"]):
        logging.error(color(f"Fatal: Interface {config['interface']} does not exist.", "red"))


     # Start processes
    tcpdump_process = capture_live_interface(
        interface=config["interface"],
        rotate_every_t_sec=rotate_every_t_seconds,  # TODO: Move this parameter to config
        temp_dir=temp_dir,
    )

    kafka_producer = ScannerKafkaProducer(
        ip_client=str(config["ip_client"]),
        addr_server=config["kafka_config"]["addr_server"],
        topic=config["kafka_config"]["topic"],
        queue_received=queue_nfstream,
        timeout_sec=10,  # TODO: Move this parameter to config
        sasl_mechanism=config["kafka_config"]["sasl_mechanism"],
        sasl_plain_username=config["kafka_config"]["sasl_plain_username"],
        sasl_plain_password=config["kafka_config"]["sasl_plain_password"],
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

                continue

            df = get_df_from_pcap(pcap_file)

            if not isinstance(df, pd.DataFrame):
                logging.error(f"Failed to process pcap: {pcap_file}. Returned data is not a pandas DataFrame, but {type(df)}.")
                continue

            data = df.to_csv()
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
