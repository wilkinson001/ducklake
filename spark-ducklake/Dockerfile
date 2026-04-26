FROM apache/spark:4.1.1-python3 AS base

USER root

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY pyproject.toml uv.lock ./
RUN uv pip install --system -r pyproject.toml

ENV PYTHONPATH="/opt/spark-ducklake/src:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip:${PYTHONPATH}"

RUN mkdir -p /home/spark && chown spark:spark /home/spark
ENV HOME="/home/spark"

USER spark

CMD ["tail", "-f", "/dev/null"]


FROM base AS test

USER root

RUN uv pip install --system --group dev -r pyproject.toml

USER spark
