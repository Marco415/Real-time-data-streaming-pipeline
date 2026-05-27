FROM flink:1.17

USER root

# Install Python + system dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    build-essential \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && pip3 install --upgrade pip

# Python dependencies
RUN pip3 install \
    apache-flink==1.17.0 \
    scikit-learn==1.2.2 \
    numpy \
    pandas \
    joblib \
    cassandra-driver \
    kafka-python