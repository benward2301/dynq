FROM debian:bookworm-slim

SHELL ["/bin/bash", "-c"]

RUN set -e; \
    apt update; \
    apt install -y build-essential zlib1g-dev curl unzip zip; \
    curl -s "https://get.sdkman.io" | bash; \
    source "/root/.sdkman/bin/sdkman-init.sh"; \
    sdk install java 23.0.1-graal; \
    curl -L https://downloads.apache.org/maven/maven-3/3.9.9/binaries/apache-maven-3.9.9-bin.tar.gz -o /tmp/maven.tar.gz; \
    tar -xzf /tmp/maven.tar.gz -C /opt; \
    curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash; \
    \. /root/.nvm/nvm.sh; \
    nvm install 22.11.0;

ENV JAVA_HOME="/root/.sdkman/candidates/java/current"
ENV PATH="/root/.sdkman/candidates/java/current/bin:$PATH"
ENV PATH="/opt/apache-maven-3.9.9/bin:$PATH"
ENV PATH="/root/.nvm/versions/node/v22.11.0/bin:$PATH"

COPY .aws ./root/.aws

WORKDIR /dynq
