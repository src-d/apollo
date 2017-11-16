FROM ubuntu:16.04

RUN apt-get update && \
    apt-get install -y --no-install-suggests --no-install-recommends \
        ca-certificates locales git python3 python3-dev libxml2 libxml2-dev \
        libsnappy1v5 libsnappy-dev libonig2 make gcc g++ curl openjdk-8-jre && \
    curl https://bootstrap.pypa.io/get-pip.py | python3 && \
    pip3 install --no-cache-dir PyStemmer bblfsh py4j modelforge parquet libMHCUDA && \
    apt-get remove -y python3-dev libxml2-dev libsnappy-dev make gcc g++ curl && \
    apt-get remove -y *-doc *-man && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    locale-gen en_US.UTF-8

# sudo mount -o bind ... bundle/*
ADD bundle/spark /spark/
ADD bundle/engine/python/sourced/engine/ /packages/sourced/engine/
ADD bundle/ml/sourced/ml/ /packages/sourced/ml/
ADD gemini/ /packages/gemini/

ENV PYTHONPATH /packages/gemini:/packages/sourced:/spark/python
ENV PYSPARK_PYTHON /usr/bin/python3
WORKDIR /packages

RUN touch /packages/sourced/__init__.py && python3 -m gemini warmup -s 'local[*]'

ENTRYPOINT ["python3", "-m", "gemini"]
