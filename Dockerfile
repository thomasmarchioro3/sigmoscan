FROM python:3.10-slim-bookworm

# System tools: installed first since they almost never change
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        iproute2 \
        tcpdump \
    && rm -rf /var/lib/apt/lists/*

# uv for dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml ./
RUN rm -rf .venv && uv sync --python python3

COPY . .

RUN TEMP_DIR=$(grep -E '^\s*TEMP_PCAP_PATH\s*=' config.ini | cut -d'=' -f2 | tr -d ' \t\r') \
    && rm -rf "$TEMPDIR" \
    && mkdir -p "$TEMP_DIR" \
    && chown tcpdump:tcpdump "$TEMP_DIR"

ENV PATH="/app/.venv/bin:$PATH"

CMD ["uv", "run", "main.py"]
