FROM rust:1.99-slim-bookworm AS build

WORKDIR /build

# Backend crate (see vm/Cargo.toml); Cargo.lock is checked in so the
# image build is reproducible with --locked.
COPY vm/Cargo.toml vm/Cargo.lock ./
COPY vm/src ./src
RUN cargo build --locked --release

FROM debian:bookworm-slim

WORKDIR /app

COPY --from=build /build/target/release/robotocore-extension-vm /app/robotocore-extension-vm

# Copy UI
COPY ui/ /app/ui/

# Copy metadata
COPY metadata.json /app/
COPY docker-compose.yaml /app/

LABEL com.docker.desktop.extension.api.version="0.3.4"
LABEL com.docker.desktop.extension.icon="robotocore.svg"

EXPOSE 8080

CMD ["/app/robotocore-extension-vm"]
