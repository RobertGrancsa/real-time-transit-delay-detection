FROM apache/flink:1.20.3-java17

# Install Python 3, pip, and OpenJDK headers — required for building PyFlink wheel
# (apache/flink:*-java17 ships a JRE only; PyFlink's build needs JDK include headers)
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 python3-pip python3-dev \
        openjdk-17-jdk-headless \
        build-essential && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Point JAVA_HOME to the system JDK (which has /include) so PyFlink build can find headers.
# Use a wildcard-resolved path that works on both arm64 and amd64.
RUN ln -s /usr/lib/jvm/java-17-openjdk-* /usr/lib/jvm/java-17-openjdk
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk

# Install PyFlink (must match the Flink version). Use --break-system-packages on Debian
# Bookworm where PEP 668 marks the system Python as externally managed.
RUN pip3 install --no-cache-dir --break-system-packages apache-flink==1.20.0 || \
    pip3 install --no-cache-dir apache-flink==1.20.0

# Copy connector JARs into Flink's lib directory so they're on the classpath
COPY flink/lib/*.jar /opt/flink/lib/

# Ensure checkpoint/savepoint directories exist and are owned by the flink user
RUN mkdir -p /opt/flink/checkpoints /opt/flink/savepoints && \
    chown -R flink:flink /opt/flink/checkpoints /opt/flink/savepoints

USER flink
