"""
Copyright (C) 2025, CEA

This program is free software; you can redistribute it and/or modify
it under the terms of the Creative Commons Attribution-NonCommercial-ShareAlike 4.0
International License.

You should have received a copy of the license along with this
program. If not, see <https://creativecommons.org/licenses/by-nc-sa/4.0/>.
"""

import configparser
import ipaddress
import os

from typing import TypedDict

def sasl_sanity_checks(
        security_protocol: str | None, 
        sasl_mechanism: str | None, 
        sasl_plain_username: str | None, 
        sasl_plain_password: str | None
    ):

    if security_protocol not in ("PLAINTEXT", "SASL_PLAINTEXT"):
        raise ValueError("Supported sasl_mechanism values: PLAINTEXT, SASL_PLAINTEXT")

    if sasl_mechanism not in ("PLAIN", None):
        raise ValueError("Supported sasl_mechanism values: PLAIN, None")

    if sasl_mechanism == "PLAIN":
        if (sasl_plain_username is None) or (sasl_plain_password is None):
            raise ValueError("sasl_mechanism is set to PLAIN but sasl_plain_username or sasl_plain_password is None")


class KafkaConfig(TypedDict):
    addr_server: str
    topic: str
    security_protocol: str | None
    sasl_mechanism: str | None
    sasl_plain_username: str | None
    sasl_plain_password: str | None

class ScannerConfig(TypedDict):
    interface: str | None
    ip_client: ipaddress.IPv4Address | ipaddress.IPv6Address
    temp_pcap_path: str
    kafka_config: KafkaConfig


def parse_config(config_file: str) -> ScannerConfig:
    config = configparser.ConfigParser()
    _ = config.read(config_file)

    interface = config["SCANNER"]["INTERFACE"]
    if interface == "default":
        interface = None

    try:
        ip_client = ipaddress.ip_address(config['KAFKA']['IP_CLIENT'])
    except ValueError:
        print(f"ValueError: IP_CLIENT in section [KAFKA] of the config is not a valid IPv4 or IPv6 address.")
        exit(-1)
   
    temp_pcap_path = config["SCANNER"]["TEMP_PCAP_PATH"]
    if not os.path.isdir(temp_pcap_path):
        os.makedirs(temp_pcap_path)

    addr_server: str = config['KAFKA']['ADDRESS_SERVER']
    topic: str = config["KAFKA"]["TOPIC"]

    # TODO: Improve type checks of sasl_mechanism using Literal
    security_protocol: str = config["KAFKA"].get("SECURITY_PROTOCOL", "PLAINTEXT")
    assert security_protocol is not None
    sasl_mechanism: str | None = config["KAFKA"].get("SASL_MECHANISM", None)
    if sasl_mechanism == "None":
        sasl_mechanism = None
    sasl_plain_username: str | None = config["KAFKA"].get("SASL_PLAIN_USERNAME", None)
    if sasl_plain_username == "None":
        sasl_plain_username = None
    sasl_plain_password: str | None = config["KAFKA"].get("SASL_PLAIN_PASSWORD", None)
    if sasl_plain_password == "None":
        sasl_plain_password = None

    sasl_sanity_checks(security_protocol, sasl_mechanism, sasl_plain_username, sasl_plain_password)

    return ScannerConfig(
        interface=interface,
        ip_client=ip_client,
        temp_pcap_path=temp_pcap_path,
        kafka_config=KafkaConfig(
            addr_server=addr_server,
            topic=topic,
            security_protocol=security_protocol,
            sasl_mechanism=sasl_mechanism,
            sasl_plain_username=sasl_plain_username,
            sasl_plain_password=sasl_plain_password,
        )
    )

