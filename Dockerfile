# Dockerfile — PyFlink-Ready Flink 1.18.0 (Full JDK + pemja fix)
FROM flink:1.18.0

USER root

# Install full JDK 11 + Python dev tools (required for pemja JNI build)
RUN apt-get update -y && \
    apt-get install -y openjdk-11-jdk python3 python3-pip python3-dev build-essential && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME early (pemja requires it)
ENV JAVA_HOME=/opt/java/openjdk

# Create include dir and copy ALL headers (fixes 'jni.h missing')
RUN mkdir -p /opt/java/openjdk/include /opt/java/openjdk/include/linux && \
    find /usr/lib/jvm -name "jni.h" -exec cp {} /opt/java/openjdk/include/ \; || true && \
    find /usr/lib/jvm -name "*.h" -path "*/include/*" -exec cp {} /opt/java/openjdk/include/ \; || true && \
    find /usr/lib/jvm -name "*.h" -path "*/include/linux/*" -exec cp {} /opt/java/openjdk/include/linux/ \; || true && \
    # Verify
    ls /opt/java/openjdk/include/jni.h && echo "Headers copied successfully"

# Upgrade pip + build tools
RUN pip3 install --upgrade pip setuptools wheel --no-cache-dir

# Install pemja FIRST (builds JNI)
RUN pip3 install pemja==0.3.0 --no-cache-dir --no-build-isolation

# Install PyFlink (now succeeds)
RUN pip3 install apache-flink==1.18.0 --no-cache-dir --no-build-isolation

# Your deps
RUN pip3 install pyyaml requests ollama --no-cache-dir

USER flink
WORKDIR /opt/flink
