FROM docker.io/rust:slim-trixie AS builder
WORKDIR /usr/src/app
COPY . .
RUN cargo build --release --package ephcom-server

FROM docker.io/debian:trixie-slim
WORKDIR /
COPY --from=builder /usr/src/app/target/release/ephcom-server ./
CMD ["./ephcom-server"]