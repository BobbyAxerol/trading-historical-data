ARG PYTHON_BASE_IMAGE=python:3.12-slim@sha256:d657ab0ade19f404a6ccc883ab399540de667aff751748ce23c07330c5a89e64
FROM ${PYTHON_BASE_IMAGE}

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements-collector.lock /app/requirements-collector.lock
RUN pip install --only-binary=:all: --require-hashes -r /app/requirements-collector.lock

COPY . /app
COPY docker/entrypoint.sh /usr/local/bin/primus-hmd-gate
RUN chmod 0555 /usr/local/bin/primus-hmd-gate

ENTRYPOINT ["/usr/local/bin/primus-hmd-gate"]
