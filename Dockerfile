FROM debian:bullseye-slim

COPY target/dynq /usr/local/bin

ENTRYPOINT ["dynq"]
