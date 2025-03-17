FROM debian:bookworm-slim

COPY target/dynq /usr/local/bin

ENV DYNQ_COMPATIBILITY_MODE=1

ENTRYPOINT ["dynq"]
