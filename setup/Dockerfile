FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl unzip ca-certificates gettext-base && \
    curl -fsSL https://install.duckdb.org | sh && \
    ln -s /root/.duckdb/cli/latest/duckdb /usr/local/bin/duckdb && \
    apt-get purge -y curl unzip && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
