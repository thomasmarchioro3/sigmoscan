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

from dotenv import dotenv_values


DEFAULT_ROTATE_EVERY_T_SEC: int = 10

SUPPORTED_SECURITY_PROTOCOLS: tuple[str, ...] = ("PLAINTEXT", "SASL_PLAINTEXT", "SASL_SSL")

# Kafka connection settings, read from the .env file (same names as in example-new-kafka/)
ENV_KEYS: tuple[str, ...] = (
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_TOPIC",
    "KAFKA_SASL_USERNAME",
    "KAFKA_SASL_PASSWORD",
    "KAFKA_CA_FILE",
    "KAFKA_CERT_FILE",
    "KAFKA_KEY_FILE",
    "KAFKA_KEY_PASSWORD",
)

# .env variables that are only used with SASL_SSL
SSL_ENV_KEYS: tuple[str, ...] = ("KAFKA_CA_FILE", "KAFKA_CERT_FILE", "KAFKA_KEY_FILE", "KAFKA_KEY_PASSWORD")

# Old keys of the [KAFKA] section of config.ini, and the .env variable that replaces each one
MOVED_TO_ENV: dict[str, str] = {
    "ADDRESS_SERVER": "KAFKA_BOOTSTRAP_SERVERS",
    "TOPIC": "KAFKA_TOPIC",
    "SASL_PLAIN_USERNAME": "KAFKA_SASL_USERNAME",
    "SASL_PLAIN_PASSWORD": "KAFKA_SASL_PASSWORD",
    "SSL_CAFILE": "KAFKA_CA_FILE",
    "SSL_CERTFILE": "KAFKA_CERT_FILE",
    "SSL_KEYFILE": "KAFKA_KEY_FILE",
    "SSL_PASSWORD": "KAFKA_KEY_PASSWORD",
}

# Shown instead of a password when the config is logged
SECRET_MASK: str = "***"


def _get_optional(section: configparser.SectionProxy, key: str) -> str | None:
    """
    Returns the value of `key`, or None if the key is missing or set to the
    string "None".
    """
    value = section.get(key, None)
    if value is None or value == "None":
        return None
    return value


def _read_env(env_file: str) -> dict[str, str | None]:
    """
    Reads the variables of `env_file` (if the file exists). A variable in
    ENV_KEYS that is also set in the environment takes the value from the
    environment, as with load_dotenv in example-new-kafka/. So Docker can pass
    the variables with --env-file instead of a file in the image.

    os.environ is not changed.
    """
    values: dict[str, str | None] = dict(dotenv_values(env_file)) if os.path.isfile(env_file) else {}
    for key in ENV_KEYS:
        if key in os.environ:
            values[key] = os.environ[key]
    return values


def _get_env(values: dict[str, str | None], key: str) -> str | None:
    """
    Returns the value of `key`, or None if it is missing, empty or set to the
    string "None".
    """
    value = values.get(key)
    if value is None or value in ("", "None"):
        return None
    return value


def _require_env(values: dict[str, str | None], key: str, env_file: str) -> str:
    value = _get_env(values, key)
    if value is None:
        raise ValueError(f"{key} is not set (in {env_file} or in the environment)")
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
            raise ValueError("SASL_MECHANISM is PLAIN but KAFKA_SASL_USERNAME or KAFKA_SASL_PASSWORD is not set")


def ssl_sanity_checks(
        ssl_cafile: str | None,
        ssl_certfile: str | None,
        ssl_keyfile: str | None,
        ssl_password: str | None,
    ):

    if (ssl_certfile is None) != (ssl_keyfile is None):
        raise ValueError("KAFKA_CERT_FILE and KAFKA_KEY_FILE must be both set or both unset")

    if ssl_password is not None and ssl_keyfile is None:
        raise ValueError("KAFKA_KEY_PASSWORD is set but KAFKA_KEY_FILE is not")

    for key, path in (("KAFKA_CA_FILE", ssl_cafile), ("KAFKA_CERT_FILE", ssl_certfile), ("KAFKA_KEY_FILE", ssl_keyfile)):
        if path is not None and not os.path.isfile(path):
            raise ValueError(f"{key}: file not found: {path}")


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


def parse_config(config_file: str, env_file: str) -> ScannerConfig:
    """
    Reads the scanner settings from `config_file` (config.ini) and the Kafka
    connection settings (address, topic, credentials, certificates) from
    `env_file` (.env) or from the environment. See ENV_KEYS.

    Relative certificate paths start from the folder of `config_file`.
    """
    config = configparser.ConfigParser()
    _ = config.read(config_file)
    env = _read_env(env_file)

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

    kafka_section = config["KAFKA"]

    # Fail on old keys: otherwise they would be ignored without notice
    moved_keys = [key for key in MOVED_TO_ENV if key in kafka_section]
    if moved_keys:
        raise ValueError(
            f"These keys of section [KAFKA] in {config_file} have moved to the .env file: "
            + ", ".join(f"{key} -> {MOVED_TO_ENV[key]}" for key in moved_keys)
        )

    addr_server = _require_env(env, "KAFKA_BOOTSTRAP_SERVERS", env_file)
    topic = _require_env(env, "KAFKA_TOPIC", env_file)

    # TODO: Improve type checks of sasl_mechanism using Literal
    security_protocol: str = kafka_section.get("SECURITY_PROTOCOL", "PLAINTEXT")
    assert security_protocol is not None
    sasl_mechanism = _get_optional(kafka_section, "SASL_MECHANISM")
    sasl_plain_username = _get_env(env, "KAFKA_SASL_USERNAME")
    sasl_plain_password = _get_env(env, "KAFKA_SASL_PASSWORD")

    sasl_sanity_checks(security_protocol, sasl_mechanism, sasl_plain_username, sasl_plain_password)

    ssl_cafile: str | None = None
    ssl_certfile: str | None = None
    ssl_keyfile: str | None = None
    ssl_password: str | None = None
    ssl_check_hostname: bool = True

    if security_protocol == "SASL_SSL":
        config_dir = os.path.dirname(os.path.abspath(config_file))
        ssl_cafile = _resolve_path(_get_env(env, "KAFKA_CA_FILE"), config_dir)
        ssl_certfile = _resolve_path(_get_env(env, "KAFKA_CERT_FILE"), config_dir)
        ssl_keyfile = _resolve_path(_get_env(env, "KAFKA_KEY_FILE"), config_dir)
        ssl_password = _get_env(env, "KAFKA_KEY_PASSWORD")
        ssl_check_hostname = _parse_bool(
            _get_optional(kafka_section, "SSL_CHECK_HOSTNAME"), "SSL_CHECK_HOSTNAME", default=True
        )

        ssl_sanity_checks(ssl_cafile, ssl_certfile, ssl_keyfile, ssl_password)
    else:
        ignored_keys = [key for key in SSL_ENV_KEYS if _get_env(env, key) is not None]
        if _get_optional(kafka_section, "SSL_CHECK_HOSTNAME") is not None:
            ignored_keys.append("SSL_CHECK_HOSTNAME")
        if ignored_keys:
            logging.warning(
                f"Ignoring {', '.join(ignored_keys)}: SECURITY_PROTOCOL is {security_protocol}, not SASL_SSL"
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


def mask_secrets(config: ScannerConfig) -> ScannerConfig:
    """
    Returns a copy of `config` where the passwords that are set are replaced by
    SECRET_MASK, so the config can be logged. Passwords that are not set stay
    None, so the log still shows whether they are configured.
    """

    def _mask(value: str | None) -> str | None:
        return None if value is None else SECRET_MASK

    kafka_config = config["kafka_config"].copy()
    kafka_config["sasl_plain_password"] = _mask(kafka_config["sasl_plain_password"])
    kafka_config["ssl_password"] = _mask(kafka_config["ssl_password"])

    masked = config.copy()
    masked["kafka_config"] = kafka_config
    return masked

