FROM apache/flink:1.20.3-java17

# Install Python 3 and pip — required for PyFlink jobs
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-dev && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install PyFlink (must match the Flink version)
RUN pip3 install --no-cache-dir apache-flink==1.20.0

# Copy connector JARs into Flink's lib directory so they're on the classpath
COPY flink/lib/*.jar /opt/flink/lib/

# Ensure checkpoint/savepoint directories exist and are owned by the flink user
RUN mkdir -p /opt/flink/checkpoints /opt/flink/savepoints && \
    chown -R flink:flink /opt/flink/checkpoints /opt/flink/savepoints

USER flink
