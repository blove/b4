FROM rust:1.93.1-slim-bookworm@sha256:5b9332190bb3b9ece73b810cd1f1e9f06343b294ce184bcb067f0747d7d333ea AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends procps \
    && rm -rf /var/lib/apt/lists/* \
    && rustup component add --toolchain 1.93.1 rustfmt clippy \
    && command -v cc \
    && command -v ar \
    && test -x /bin/kill \
    && groupadd --gid 10001 b4run \
    && useradd --uid 10001 --gid 10001 --create-home --shell /usr/sbin/nologin b4run \
    && install -d -m 0700 -o 10001 -g 10001 /workspace /opt/b4/cargo \
    && chmod -R a-w /usr/local/rustup /usr/local/cargo

ENV CARGO_HOME=/opt/b4/cargo
WORKDIR /workspace
COPY --chown=10001:10001 Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY --chown=10001:10001 crates/ crates/
COPY --chown=10001:10001 contracts/agent-protocol/examples/ contracts/agent-protocol/examples/
COPY --chown=10001:10001 conformance/scenarios/ conformance/scenarios/
COPY --chown=10001:10001 conformance/reference/recordings/ conformance/reference/recordings/
COPY --chown=10001:10001 spec/ spec/
USER 10001:10001
RUN cargo fetch --locked \
    && cargo build --release -p b4-cli --bin b4run --locked --offline

FROM builder AS qualification
USER 0:0
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 \
    && rm -rf /var/lib/apt/lists/* \
    && python3 -c 'import sqlite3; print(sqlite3.sqlite_version)'
COPY --chown=0:0 --chmod=0555 scripts/container_snapshot.py /opt/b4/container_snapshot.py
COPY --chown=0:0 --chmod=0555 scripts/container_initialize_volume.sh /opt/b4/container_initialize_volume.sh
USER 10001:10001
CMD ["/bin/sleep", "infinity"]

FROM debian:bookworm-slim@sha256:3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251 AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 b4run \
    && useradd --uid 10001 --gid 10001 --create-home --shell /usr/sbin/nologin b4run \
    && install -d -m 0700 -o 10001 -g 10001 /var/lib/b4run
COPY --from=builder --chown=0:0 --chmod=0555 /workspace/target/release/b4run /usr/local/bin/b4run
ENV B4RUN_PORT=3001 \
    B4RUN_DATA_DIR=/var/lib/b4run/data \
    B4RUN_SHUTDOWN_WAIT_MS=30000
USER 10001:10001
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=5s --timeout=4s --start-period=5s --retries=3 \
    CMD ["curl", "--disable", "--fail", "--silent", "--output", "/dev/null", "--noproxy", "*", "--connect-timeout", "2", "--max-time", "3", "http://127.0.0.1:3001/readyz"]
ENTRYPOINT ["/usr/local/bin/b4run"]
CMD ["serve", "--demo"]
