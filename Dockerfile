# The underlying base is ubuntu:16.04
FROM nvidia/cuda:8.0-runtime

# NVIDIA driver version must match the host!
ENV DRIVER_VERSION 384.69
RUN mkdir -p /opt/nvidia && cd /opt/nvidia/ \
    && apt-get update && apt-get install -y wget module-init-tools && apt-get clean && rm -rf /var/lib/apt/lists/* \
    && wget http://us.download.nvidia.com/XFree86/Linux-x86_64/${DRIVER_VERSION}/NVIDIA-Linux-x86_64-${DRIVER_VERSION}.run -O /opt/nvidia/driver.run \
    && chmod +x /opt/nvidia/driver.run \
    && /opt/nvidia/driver.run -s --no-nvidia-modprobe --no-kernel-module --no-nouveau-check --no-distro-scripts --no-opengl-files --no-kernel-module-source \
    && rm -rf /opt/nvidia && apt-get purge -y module-init-tools && apt-get autoremove -y

RUN apt-get update && \
    apt-get install -y --no-install-suggests --no-install-recommends \
        ca-certificates locales git python3 libpython3.5 python3-dev \
        libgomp1 libxml2 libxml2-dev zlib1g-dev \
        libsnappy1v5 libsnappy-dev libonig2 make gcc g++ curl openjdk-8-jre && \
    curl https://bootstrap.pypa.io/get-pip.py | python3 && \
    pip3 install --no-cache-dir PyStemmer bblfsh py4j==0.10.4 modelforge parquet jinja2 libMHCUDA datasketch cassandra_driver python-igraph numpy humanize pygments && \
    apt-get remove -y python3-dev libxml2-dev libsnappy-dev zlib1g-dev make gcc g++ curl && \
    apt-get remove -y *-doc *-man >/dev/null && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    locale-gen en_US.UTF-8

# sudo mount -o bind ... bundle/*
ADD bundle/spark /spark/
ADD bundle/engine/python /bundle/sourced/engine/
ADD bundle/ml /bundle/sourced/ml/

ADD apollo/ /packages/apollo/apollo/
ADD setup.py /packages/apollo

ENV PYTHONPATH /packages:/spark/python
ENV LANG en_US.UTF-8
WORKDIR /packages

RUN echo '0.5.2' > /bundle/sourced/engine/version.txt && pip3 install -e /bundle/sourced/engine/
RUN pip3 install -e /bundle/sourced/ml/
RUN pip3 install --no-deps -e apollo/ && apollo warmup -s 'local[*]'

ENTRYPOINT ["apollo"]
