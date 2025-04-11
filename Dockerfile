FROM debian:bookworm-slim

ARG TARGETARCH

RUN apt update && apt install -y wget
RUN wget -O dynq.gz https://github.com/benward2301/dynq/releases/download/v0.1.0/dynq-0.1.0-${TARGETARCH}.gz && \
    gzip -d dynq.gz && \
    chmod +x dynq

ENTRYPOINT ["./dynq"]
