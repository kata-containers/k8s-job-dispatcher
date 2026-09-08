# Copyright (c) 2026 Kata Contributors
#
# SPDX-License-Identifier: Apache-2.0
#
# Static via musl on amd64 and arm64. ppc64le and s390x link against glibc and
# take a runtime image providing it: s390x has no musl std published at all, and
# musl's ppc64le support is a variable nobody can test here, since no CI runner
# executes that architecture natively.
#
# Toolchains are pinned to $BUILDPLATFORM: nothing here runs a target binary, and
# emulating rustc cost an order of magnitude - 48 minutes for s390x alone.

# The only place the compiler is named: CI installs this same version, and
# dependabot bumps it here. Not the MSRV, which is Cargo.toml's rust-version.
FROM --platform=$BUILDPLATFORM rust:1.98.0-trixie AS rust-base
ENV TARGET_SYSROOT=""

# ring compiles C, so every target needs a C compiler emitting its code.

# The one target that matches the build host, so stock musl-gcc suffices.
FROM rust-base AS toolchain-amd64
ENV RUST_TARGET=x86_64-unknown-linux-musl
# hadolint ignore=DL3008
RUN apt-get update && \
	apt-get install -y --no-install-recommends musl-tools && \
	rm -rf /var/lib/apt/lists/*

# Debian ships no aarch64 musl-gcc. clang cross compiles against musl-dev:arm64's
# headers, and rust-lld links against the CRT in the target's prebuilt std, which
# is what avoids needing a full musl sysroot.
FROM rust-base AS toolchain-arm64
ENV RUST_TARGET=aarch64-unknown-linux-musl
ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=rust-lld
ENV CC_aarch64_unknown_linux_musl=clang
ENV CFLAGS_aarch64_unknown_linux_musl="--target=aarch64-unknown-linux-musl -isystem /usr/include/aarch64-linux-musl"
# hadolint ignore=DL3008
RUN dpkg --add-architecture arm64 && apt-get update && \
	apt-get install -y --no-install-recommends clang lld musl-dev:arm64 && \
	rm -rf /var/lib/apt/lists/*

FROM rust-base AS toolchain-ppc64le
ENV RUST_TARGET=powerpc64le-unknown-linux-gnu
ENV TARGET_SYSROOT=/usr/powerpc64le-linux-gnu
ENV CARGO_TARGET_POWERPC64LE_UNKNOWN_LINUX_GNU_LINKER=powerpc64le-linux-gnu-gcc
ENV CC_powerpc64le_unknown_linux_gnu=powerpc64le-linux-gnu-gcc
# hadolint ignore=DL3008
RUN apt-get update && \
	apt-get install -y --no-install-recommends crossbuild-essential-ppc64el && \
	rm -rf /var/lib/apt/lists/*

FROM rust-base AS toolchain-s390x
ENV RUST_TARGET=s390x-unknown-linux-gnu
ENV TARGET_SYSROOT=/usr/s390x-linux-gnu
ENV CARGO_TARGET_S390X_UNKNOWN_LINUX_GNU_LINKER=s390x-linux-gnu-gcc
ENV CC_s390x_unknown_linux_gnu=s390x-linux-gnu-gcc
# hadolint ignore=DL3008
RUN apt-get update && \
	apt-get install -y --no-install-recommends crossbuild-essential-s390x && \
	rm -rf /var/lib/apt/lists/*

# Resolves to a stage above, which hadolint cannot see.
# hadolint ignore=DL3006
FROM toolchain-${TARGETARCH} AS builder

WORKDIR /src

RUN rustup target add "${RUST_TARGET}"

# Dependencies resolve from the manifests alone, so a placeholder entry point
# keeps them in a layer that survives source changes.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src && echo 'fn main() {}' > src/main.rs && \
	cargo build --release --locked --target "${RUST_TARGET}" && \
	rm -rf src

COPY src ./src

# COPY preserves the context's mtimes, so the real entry point has to be made
# newer than the placeholder cargo already built.
RUN touch src/main.rs && \
	cargo build --release --locked --target "${RUST_TARGET}" && \
	cp "target/${RUST_TARGET}/release/k8s-job-dispatcher" /usr/local/bin/k8s-job-dispatcher

# /usr/lib here is the build host's, so a glob would ship an amd64 libgcc in an
# s390x image and the binary would not start. Empty for the static musl targets.
RUN mkdir -p /staging && \
	if [ -n "${TARGET_SYSROOT}" ]; then \
		cp "${TARGET_SYSROOT}/lib/libgcc_s.so.1" /staging/; \
	fi

# One text for every architecture, so it builds once.
FROM rust-base AS notices

WORKDIR /src

# Ahead of the manifests, so a dependency bump does not rebuild the tool. The
# binary sits behind `cli`.
RUN cargo install cargo-about --version 0.9.2 --locked --features cli

# Needs an entry point to exist, but never compiles one.
COPY Cargo.toml Cargo.lock about.toml about.hbs ./
RUN mkdir -p src && echo 'fn main() {}' > src/main.rs && \
	cargo about generate about.hbs -o THIRD-PARTY-NOTICES.txt && \
	rm -rf src

# Lets a release ship the binary without anyone unpacking an image:
# --target binary --output type=local.
FROM scratch AS binary
COPY --from=builder /usr/local/bin/k8s-job-dispatcher /
COPY --from=notices /src/THIRD-PARTY-NOTICES.txt /
COPY LICENSE /

# distroless publishes only rolling tags, so :latest is how it is consumed.
# hadolint ignore=DL3007
FROM gcr.io/distroless/static-debian13:latest AS runtime-amd64
# hadolint ignore=DL3007
FROM gcr.io/distroless/static-debian13:latest AS runtime-arm64

# `static` plus the loader and glibc the dynamic builds need; nossl because kube
# uses rustls. It has no libgcc, where Rust's unwinder lives, and the prebuilt
# libstd records that dependency - without this copy the binary will not start.
# hadolint ignore=DL3007
FROM gcr.io/distroless/base-nossl-debian13:latest AS runtime-glibc
COPY --from=builder /staging/libgcc_s.so.1 /usr/lib/

FROM runtime-glibc AS runtime-ppc64le
FROM runtime-glibc AS runtime-s390x

# hadolint ignore=DL3006
FROM runtime-${TARGETARCH}

COPY --from=builder /usr/local/bin/k8s-job-dispatcher /usr/bin/k8s-job-dispatcher

# Where a licence scanner looks.
COPY --from=notices /src/THIRD-PARTY-NOTICES.txt /usr/share/doc/k8s-job-dispatcher/
COPY LICENSE /usr/share/doc/k8s-job-dispatcher/

USER 65532:65532

ENTRYPOINT ["/usr/bin/k8s-job-dispatcher"]
