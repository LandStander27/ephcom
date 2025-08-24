FROM rust:slim-trixie AS builder
WORKDIR /usr/src/app
COPY . .
RUN cargo build --release --package ephcom-server

FROM debian:trixie-slim
WORKDIR /
COPY --from=builder /usr/src/app/target/release/ephcom-server ./
CMD ["./ephcom-server"]