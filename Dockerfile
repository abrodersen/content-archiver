FROM rust:1.58-bullseye as builder
WORKDIR /usr/src/app
COPY . ./
RUN cargo build --release

FROM debian:bullseye-slim

RUN apt-get update \
    && apt-get install -y ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/app/target/release/content-archiver /usr/src/app/
CMD ["/usr/src/app/content-archiver"]