FROM ubuntu:16.04

# sudo mount -o bind

ADD bundle/engine/python /packages/engine/
ADD bundle/spark /spark/

RUN rm -rf package/ast2vec/tests && \
    apt-get update && \
    apt-get install -y --no-install-suggests --no-install-recommends \
        ca-certificates locales git python3 python3-dev libxml2 libxml2-dev \
        libonig2 make gcc curl openjdk-8-jre && \
    curl https://bootstrap.pypa.io/get-pip.py | python3 && \
    pip3 install --no-cache-dir PyStemmer bblfsh py4j modelforge && \
    apt-get remove -y python3-dev libxml2-dev make gcc curl && \
    apt-get remove -y *-doc *-man && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    locale-gen en_US.UTF-8

ADD bundle/ast2vec/ /packages/ast2vec/
ADD gemini/ /packages/gemini/

ENV PYTHONPATH /packages/gemini:/packages/ast2vec:/packages/engine:/spark/python
ENV PYSPARK_PYTHON /usr/bin/python3
WORKDIR /packages

RUN python3 -m gemini warmup -s 'local[*]'

ENTRYPOINT ["python3", "-m", "gemini"]
