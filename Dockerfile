FROM rust:1.82-alpine AS builder
RUN apk add --no-cache musl-dev openssl-dev pkgconfig
WORKDIR /app
COPY Cargo.toml .
COPY src/ src/
RUN cargo build --release --bin cognitive-node

FROM alpine:3.20
RUN apk add --no-cache openssl ca-certificates libgcc
WORKDIR /app
COPY --from=builder /app/target/release/cognitive-node .
EXPOSE 9001
CMD ["./cognitive-node"]
