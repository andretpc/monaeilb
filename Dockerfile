FROM clux/muslrust:1.72.0 AS builder

WORKDIR /app

COPY . .

RUN cargo build --release

FROM gcr.io/distroless/static-debian11

COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/monaeilb /usr/local/bin/

CMD ["monaeilb"]