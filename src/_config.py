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
import logging
import os

from typing import TypedDict


DEFAULT_ROTATE_EVERY_T_SEC: int = 10

SUPPORTED_SECURITY_PROTOCOLS: tuple[str, ...] = ("PLAINTEXT", "SASL_PLAINTEXT", "SASL_SSL")

# Keys of the [KAFKA] section that are only used with SASL_SSL
SSL_KEYS: tuple[str, ...] = ("SSL_CAFILE", "SSL_CERTFILE", "SSL_KEYFILE", "SSL_PASSWORD", "SSL_CHECK_HOSTNAME")


def _get_optional(section: configparser.SectionProxy, key: str) -> str | None:
    """
    Returns the value of `key`, or None if the key is missing or set to the
    string "None".
    """
    value = section.get(key, None)
    if value is None or value == "None":
        return None
    return value


def _resolve_path(path: str | None, base_dir: str) -> str | None:
    """
    Makes a relative path absolute, starting from `base_dir` (the folder of the
    config file), so the result does not depend on the current directory.
    """
    if path is None:
        return None
    if not os.path.isabs(path):
        path = os.path.join(base_dir, path)
    return os.path.normpath(path)


def _parse_bool(value: str | None, key: str, default: bool) -> bool:
    if value is None:
        return default
    states = configparser.ConfigParser.BOOLEAN_STATES
    if value.lower() not in states:
        raise ValueError(f"{key} in section [KAFKA] must be true or false, got '{value}'")
    return states[value.lower()]


def sasl_sanity_checks(
        security_protocol: str | None,
        sasl_mechanism: str | None,
        sasl_plain_username: str | None,
        sasl_plain_password: str | None
    ):

    if security_protocol not in SUPPORTED_SECURITY_PROTOCOLS:
        raise ValueError(
            f"Supported security_protocol values: {', '.join(SUPPORTED_SECURITY_PROTOCOLS)}, got '{security_protocol}'"
        )

    if sasl_mechanism not in ("PLAIN", None):
        raise ValueError("Supported sasl_mechanism values: PLAIN, None")

    if sasl_mechanism == "PLAIN":
        if (sasl_plain_username is None) or (sasl_plain_password is None):
            raise ValueError("sasl_mechanism is set to PLAIN but sasl_plain_username or sasl_plain_password is None")


def ssl_sanity_checks(
        ssl_cafile: str | None,
        ssl_certfile: str | None,
        ssl_keyfile: str | None,
        ssl_password: str | None,
    ):

    if (ssl_certfile is None) != (ssl_keyfile is None):
        raise ValueError("SSL_CERTFILE and SSL_KEYFILE in section [KAFKA] must be both set or both unset")

    if ssl_password is not None and ssl_keyfile is None:
        raise ValueError("SSL_PASSWORD is set in section [KAFKA] but SSL_KEYFILE is not")

    for key, path in (("SSL_CAFILE", ssl_cafile), ("SSL_CERTFILE", ssl_certfile), ("SSL_KEYFILE", ssl_keyfile)):
        if path is not None and not os.path.isfile(path):
            raise ValueError(f"{key} in section [KAFKA]: file not found: {path}")


class KafkaConfig(TypedDict):
    addr_server: str
    topic: str
    security_protocol: str | None
    sasl_mechanism: str | None
    sasl_plain_username: str | None
    sasl_plain_password: str | None
    ssl_cafile: str | None
    ssl_certfile: str | None
    ssl_keyfile: str | None
    ssl_password: str | None
    ssl_check_hostname: bool

class ScannerConfig(TypedDict):
    interface: str | None
    ip_client: ipaddress.IPv4Address | ipaddress.IPv6Address
    temp_pcap_path: str
    rotate_every_t_sec: int
    kafka_config: KafkaConfig


def parse_rotate_every_t_sec(value: str | None) -> int:
    """
    Parses ROTATE_EVERY_T_SEC. tcpdump -G only accepts whole seconds, so the
    value must be an integer >= 1. Returns the default if the value is missing.
    """
    if value is None:
        return DEFAULT_ROTATE_EVERY_T_SEC

    try:
        seconds = int(value)
    except ValueError:
        raise ValueError(f"ROTATE_EVERY_T_SEC in section [SCANNER] must be a whole number of seconds, got '{value}'")

    if seconds < 1:
        raise ValueError(f"ROTATE_EVERY_T_SEC in section [SCANNER] must be at least 1, got {seconds}")

    return seconds


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

    rotate_every_t_sec = parse_rotate_every_t_sec(config["SCANNER"].get("ROTATE_EVERY_T_SEC"))

    addr_server: str = config['KAFKA']['ADDRESS_SERVER']
    topic: str = config["KAFKA"]["TOPIC"]

    kafka_section = config["KAFKA"]

    # TODO: Improve type checks of sasl_mechanism using Literal
    security_protocol: str = kafka_section.get("SECURITY_PROTOCOL", "PLAINTEXT")
    assert security_protocol is not None
    sasl_mechanism = _get_optional(kafka_section, "SASL_MECHANISM")
    sasl_plain_username = _get_optional(kafka_section, "SASL_PLAIN_USERNAME")
    sasl_plain_password = _get_optional(kafka_section, "SASL_PLAIN_PASSWORD")

    sasl_sanity_checks(security_protocol, sasl_mechanism, sasl_plain_username, sasl_plain_password)

    ssl_cafile: str | None = None
    ssl_certfile: str | None = None
    ssl_keyfile: str | None = None
    ssl_password: str | None = None
    ssl_check_hostname: bool = True

    if security_protocol == "SASL_SSL":
        config_dir = os.path.dirname(os.path.abspath(config_file))
        ssl_cafile = _resolve_path(_get_optional(kafka_section, "SSL_CAFILE"), config_dir)
        ssl_certfile = _resolve_path(_get_optional(kafka_section, "SSL_CERTFILE"), config_dir)
        ssl_keyfile = _resolve_path(_get_optional(kafka_section, "SSL_KEYFILE"), config_dir)
        ssl_password = _get_optional(kafka_section, "SSL_PASSWORD")
        ssl_check_hostname = _parse_bool(
            _get_optional(kafka_section, "SSL_CHECK_HOSTNAME"), "SSL_CHECK_HOSTNAME", default=True
        )

        ssl_sanity_checks(ssl_cafile, ssl_certfile, ssl_keyfile, ssl_password)
    else:
        ignored_keys = [key for key in SSL_KEYS if _get_optional(kafka_section, key) is not None]
        if ignored_keys:
            logging.warning(
                f"Ignoring {', '.join(ignored_keys)} in section [KAFKA]: SECURITY_PROTOCOL is {security_protocol}, not SASL_SSL"
            )

    return ScannerConfig(
        interface=interface,
        ip_client=ip_client,
        temp_pcap_path=temp_pcap_path,
        rotate_every_t_sec=rotate_every_t_sec,
        kafka_config=KafkaConfig(
            addr_server=addr_server,
            topic=topic,
            security_protocol=security_protocol,
            sasl_mechanism=sasl_mechanism,
            sasl_plain_username=sasl_plain_username,
            sasl_plain_password=sasl_plain_password,
            ssl_cafile=ssl_cafile,
            ssl_certfile=ssl_certfile,
            ssl_keyfile=ssl_keyfile,
            ssl_password=ssl_password,
            ssl_check_hostname=ssl_check_hostname,
        )
    )

