"""
Copyright (C) 2025, CEA

This program is free software; you can redistribute it and/or modify
it under the terms of the Creative Commons Attribution-NonCommercial-ShareAlike 4.0
International License.

You should have received a copy of the license along with this
program. If not, see <https://creativecommons.org/licenses/by-nc-sa/4.0/>.
"""

import os

from kafka.admin.client import logging
import nfstream
import pandas as pd

from ._color import color


DEFAULT_ATTRIBUTES = [
    "id",
    "expiration_id",
    "src_ip",
    "src_mac",
    "src_oui",
    "src_port",
    "dst_ip",
    "dst_mac",
    "dst_oui",
    "dst_port",
    "protocol",
    "ip_version",
    "vlan_id",
    "bidirectional_first_seen_ms",
    "bidirectional_last_seen_ms",
    "bidirectional_duration_ms",
    "bidirectional_packets",
    "bidirectional_bytes",
    "src2dst_first_seen_ms",
    "src2dst_last_seen_ms",
    "src2dst_duration_ms",
    "src2dst_packets",
    "src2dst_bytes",
    "dst2src_first_seen_ms",
    "dst2src_last_seen_ms",
    "dst2src_duration_ms",
    "dst2src_packets",
    "dst2src_bytes",
]

# available if decode_tunnel=True
TUNNEL_ATTRIBUTES = [
    "tunnel_id",
]

# available if n_dissections > 0
LAYER7_ATTRIBUTES = [
    "application_name",
    "application_category_name",
    "application_is_guessed",
    "application_confidence",
    "requested_server_name",
    "client_fingerprint",
    "server_fingerprint",
    "user_agent",
    "content_type",
]


# available if statistical analysis = True
STATISTICAL_ANALYSIS_ATTRIBUTES = [
    "bidirectional_min_ps",
    "bidirectional_mean_ps",
    "bidirectional_stddev_ps",
    "bidirectional_max_ps",
    "src2dst_min_ps",
    "src2dst_mean_ps",
    "src2dst_stddev_ps",
    "src2dst_max_ps",
    "dst2src_min_ps",
    "dst2src_mean_ps",
    "dst2src_stddev_ps",
    "dst2src_max_ps",
    "bidirectional_min_piat_ms",
    "bidirectional_mean_piat_ms",
    "bidirectional_stddev_piat_ms",
    "bidirectional_max_piat_ms",
    "src2dst_min_piat_ms",
    "src2dst_mean_piat_ms",
    "src2dst_stddev_piat_ms",
    "src2dst_max_piat_ms",
    "dst2src_min_piat_ms",
    "dst2src_mean_piat_ms",
    "dst2src_stddev_piat_ms",
    "dst2src_max_piat_ms",
    "bidirectional_syn_packets",
    "bidirectional_cwr_packets",
    "bidirectional_ece_packets",
    "bidirectional_urg_packets",
    "bidirectional_ack_packets",
    "bidirectional_psh_packets",
    "bidirectional_rst_packets",
    "bidirectional_fin_packets",
    "src2dst_syn_packets",
    "src2dst_cwr_packets",
    "src2dst_ece_packets",
    "src2dst_urg_packets",
    "src2dst_ack_packets",
    "src2dst_psh_packets",
    "src2dst_rst_packets",
    "src2dst_fin_packets",
    "dst2src_syn_packets",
    "dst2src_cwr_packets",
    "dst2src_ece_packets",
    "dst2src_urg_packets",
    "dst2src_ack_packets",
    "dst2src_psh_packets",
    "dst2src_rst_packets",
    "dst2src_fin_packets",
]

ALL_ATTRIBUTES = (
    DEFAULT_ATTRIBUTES
    + TUNNEL_ATTRIBUTES
    + LAYER7_ATTRIBUTES
    + STATISTICAL_ANALYSIS_ATTRIBUTES
)


def _contains_attributes(use_attributes: list, reference_attributes: list) -> bool:
    """
    Returns true if the intersection between `use_attributes` and
    `references_attributes` is non-empty
    """
    return len(set(use_attributes).intersection(reference_attributes)) > 0


def get_df_from_pcap(
    pcap_file: str,
    use_attributes: list | None = None,
) -> pd.DataFrame:
    """
    Runs nfstream on a given pcap file and yields a stream of dictionaries,
    each representing a connection in the pcap file. The stream will contain
    only the attributes specified in `use_attributes`. If `use_attributes` is
    None, the stream will contain all available attributes.

    Args:
        pcap_file (str): Path to the pcap file to read from.
        use_attributes (list | None): List of attributes to use. If None, all
        available attributes will be used.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the stream of connections.

    Raises:
        ValueError: If any of the specified attributes are invalid.
    """
    if use_attributes is None:
        use_attributes = ALL_ATTRIBUTES
        use_attributes.append("timestamp")

    use_attributes = list(set(use_attributes))

    if any(attr not in ALL_ATTRIBUTES for attr in use_attributes if attr != "timestamp"):
        raise ValueError(
            f"Invalid attribute(s): {', '.join(set(use_attributes) - set(ALL_ATTRIBUTES) - {'timestamp'})}"
        )

    decode_tunnels = _contains_attributes(use_attributes, TUNNEL_ATTRIBUTES)
    n_dissections = 20 if _contains_attributes(use_attributes, LAYER7_ATTRIBUTES) else 0
    statistical_analysis = _contains_attributes(
        use_attributes, STATISTICAL_ANALYSIS_ATTRIBUTES
    )

    streamer = nfstream.NFStreamer(
        source=pcap_file,
        decode_tunnels=decode_tunnels,
        bpf_filter=None,
        promiscuous_mode=True,
        snapshot_length=1536,
        idle_timeout=120,
        active_timeout=1800,
        accounting_mode=0,
        udps=None,
        n_dissections=n_dissections,
        statistical_analysis=statistical_analysis,
        splt_analysis=0,
        n_meters=0,
        max_nflows=0,
        performance_report=0,
        system_visibility_mode=0,
        system_visibility_poll_ms=100,
    )

    try:
        streamer.to_csv("/tmp/nfstream-temp.csv")
    except ValueError:
        pass

    conn_df = None
    if os.path.exists("/tmp/nfstream-temp.csv"):
        conn_df = pd.read_csv("/tmp/nfstream-temp.csv", index_col=0)
        conn_df["timestamp"] = conn_df["bidirectional_first_seen_ms"]
        os.remove("/tmp/nfstream-temp.csv")

    if conn_df is None:
        conn_df = pd.DataFrame(columns=use_attributes)
        logging.info(color(f"WARNING: Empty dataframe created for {pcap_file} (conn_df was None)", "yellow"))
    elif len(conn_df) == 0:
        conn_df = pd.DataFrame(columns=use_attributes)
        logging.info(color(f"WARNING: Empty dataframe created for {pcap_file} (conn_df had len 0)", "yellow"))
    else:
        conn_df = conn_df.reindex(columns=use_attributes)

    return conn_df
