FROM ubuntu:24.04 AS base

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:${PATH}"

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libgfortran5 \
    libgomp1 \
    libopenmpi-dev \
    libstdc++6 \
    libx11-6 \
    libxext6 \
    libxrender1 \
    openmpi-bin \
    python3 \
    python3-venv \
    tini \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
COPY config ./config

RUN python3 -m venv /opt/venv \
    && /opt/venv/bin/pip install --upgrade pip \
    && /opt/venv/bin/pip install .

FROM base AS mock-runtime

ENV ORCA_BACKEND=mock \
    RESULTS_ROOT=/app/data

RUN mkdir -p /app/data

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["orca-b12-service", "--config", "/app/config/b12.toml"]

FROM base AS runtime

COPY vendor/orca/ /opt/orca/

RUN test -x /opt/orca/orca \
    && mkdir -p /app/data

ENV ORCA_BINARY=/opt/orca/orca \
    RESULTS_ROOT=/app/data \
    PATH="/opt/orca:/opt/venv/bin:${PATH}"

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["orca-b12-service", "--config", "/app/config/b12.toml"]

FROM mock-runtime AS ci
