"""
Copyright (C) 2025, CEA

This program is free software; you can redistribute it and/or modify
it under the terms of the Creative Commons Attribution-NonCommercial-ShareAlike 4.0
International License.

You should have received a copy of the license along with this
program. If not, see <https://creativecommons.org/licenses/by-nc-sa/4.0/>.
"""

import configparser
from typing import TypedDict

def sasl_sanity_checks(
        sasl_mechanism: str | None, 
        sasl_plain_username: str | None, 
        sasl_plain_password: str | None
    ):

    if sasl_mechanism not in ("PLAIN", None):
        raise ValueError("Supported sasl_mechanism values: PLAIN, None")

    if sasl_mechanism == "PLAIN":
        if (sasl_plain_username is None) or (sasl_plain_password is None):
            raise ValueError("sasl_mechanism is set to PLAIN but sasl_plain_username or sasl_plain_password is None")


class ConsumerConfig(TypedDict):
    addr_server: str
    topic: str
    sasl_mechanism: str | None
    sasl_plain_username: str | None
    sasl_plain_password: str | None


def parse_config(config_file: str) -> ConsumerConfig:
    config = configparser.ConfigParser()
    _ = config.read(config_file)

    # TODO: Improve type checks of sasl_mechanism using Literal
    sasl_mechanism: str | None = config["KAFKA"].get("SASL_MECHANISM", None)
    if sasl_mechanism == "None":
        sasl_mechanism = None
    sasl_plain_username: str | None = config["KAFKA"].get("SASL_PLAIN_USERNAME", None)
    if sasl_plain_username == "None":
        sasl_plain_username = None
    sasl_plain_password: str | None = config["KAFKA"].get("SASL_PLAIN_PASSWORD", None)
    if sasl_plain_password == "None":
        sasl_plain_password = None

    sasl_sanity_checks(sasl_mechanism, sasl_plain_username, sasl_plain_password)
    
    return ConsumerConfig(
        addr_server=config["KAFKA"]["ADDRESS_SERVER"],
        topic=config["KAFKA"]["TOPIC_TRAFFIC"],
        sasl_mechanism=sasl_mechanism,
        sasl_plain_username=sasl_plain_username,
        sasl_plain_password=sasl_plain_password,
    )
