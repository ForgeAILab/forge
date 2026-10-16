FROM node:24-bookworm AS web-builder

WORKDIR /app/web

RUN corepack enable && corepack prepare pnpm@10 --activate

COPY web/package.json web/pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile

COPY web/ ./
RUN pnpm build

FROM rust:1.88-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY crates/forge-cli/Cargo.toml crates/forge-cli/Cargo.toml
COPY crates/forge-client/Cargo.toml crates/forge-client/Cargo.toml
COPY crates/api/Cargo.toml crates/api/Cargo.toml
COPY crates/api-types/Cargo.toml crates/api-types/Cargo.toml
COPY crates/db/Cargo.toml crates/db/Cargo.toml
COPY crates/services/Cargo.toml crates/services/Cargo.toml
COPY crates/executors/Cargo.toml crates/executors/Cargo.toml
COPY crates/cli-adapters/Cargo.toml crates/cli-adapters/Cargo.toml
COPY crates/workspace/Cargo.toml crates/workspace/Cargo.toml
COPY crates/git/Cargo.toml crates/git/Cargo.toml
COPY crates/review/Cargo.toml crates/review/Cargo.toml
COPY crates/events/Cargo.toml crates/events/Cargo.toml
COPY crates/mcp-server/Cargo.toml crates/mcp-server/Cargo.toml
COPY crates/config/Cargo.toml crates/config/Cargo.toml

RUN mkdir -p crates/forge-cli/src crates/forge-client/src crates/api/src \
    crates/api-types/src crates/db/src crates/services/src crates/executors/src \
    crates/cli-adapters/src crates/workspace/src crates/git/src crates/review/src \
    crates/events/src crates/mcp-server/src crates/config/src

RUN echo 'fn main(){}' > crates/forge-cli/src/main.rs \
    && echo 'fn main(){}' > crates/forge-client/src/main.rs \
    && echo '' > crates/api/src/lib.rs \
    && echo '' > crates/api-types/src/lib.rs \
    && echo '' > crates/db/src/lib.rs \
    && echo '' > crates/services/src/lib.rs \
    && echo '' > crates/executors/src/lib.rs \
    && echo '' > crates/cli-adapters/src/lib.rs \
    && echo '' > crates/workspace/src/lib.rs \
    && echo '' > crates/git/src/lib.rs \
    && echo '' > crates/review/src/lib.rs \
    && echo '' > crates/events/src/lib.rs \
    && echo '' > crates/mcp-server/src/lib.rs \
    && echo '' > crates/config/src/lib.rs

ENV FORGE_SKIP_WEB_BUILD=1
RUN cargo build --release -p forge-cli -p forge-client 2>/dev/null || true

COPY crates/ crates/
RUN touch crates/*/src/*.rs crates/**/src/*.rs 2>/dev/null || true
RUN cargo build --release -p forge-cli -p forge-client

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    git \
    tini \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/forge /usr/local/bin/forge
COPY --from=builder /app/target/release/forge-ctl /usr/local/bin/forge-ctl
COPY --from=web-builder /app/web/dist /usr/local/share/forge/web/dist

RUN mkdir -p /data
ENV FORGE_DATA_DIR=/data
ENV FORGE_WEB_DIST_DIR=/usr/local/share/forge/web/dist

EXPOSE 8080

ENTRYPOINT ["/usr/bin/tini", "--", "forge"]
CMD ["--data-dir", "/data"]
