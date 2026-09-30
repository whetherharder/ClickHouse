#!/usr/bin/env bash
# Fork-only build for GitHub-hosted runners (4 vCPU, 16 GB, 6 h per job).
# Runs inside clickhouse/fasttest with the repository mounted at /ClickHouse, like upstream Fast test.
# The feature set is Fast test's plus what Iceberg needs (S3, Avro, Parquet); Rust is off to save time.
# Release, not RelWithDebInfo: linking the full binary with debug info does not fit in 16 GB.
# Exit codes: 0 - binary built, 124 - time budget spent (sccache keeps progress, the next job continues).
set -euo pipefail

BUILD_DIR=/ClickHouse/ci/tmp/build
BUDGET_MIN=${BUILD_BUDGET_MIN:-300}

export SCCACHE_DIR=/ClickHouse/ci/tmp/sccache
export SCCACHE_CACHE_SIZE=${SCCACHE_CACHE_SIZE:-7G}
export SCCACHE_IDLE_TIMEOUT=0

git config --global --add safe.directory '*'
sccache --start-server || echo "WARNING: sccache did not start, building without cache"

if [ ! -f "$BUILD_DIR/build.ninja" ]; then
    cmake -G Ninja -S /ClickHouse -B "$BUILD_DIR" \
        -DCMAKE_C_COMPILER=clang-22 -DCMAKE_CXX_COMPILER=clang++-22 \
        -DCMAKE_BUILD_TYPE=Release \
        -DCOMPILER_CACHE=sccache \
        -DENABLE_LIBRARIES=0 -DENABLE_TESTS=0 -DENABLE_UTILS=0 -DENABLE_THINLTO=0 \
        -DENABLE_NURAFT=1 -DENABLE_SIMDJSON=1 -DENABLE_JEMALLOC=1 -DENABLE_LIBURING=1 -DENABLE_YAML_CPP=1 \
        -DENABLE_RUST=0 \
        -DENABLE_AWS_S3=1 -DENABLE_AVRO=1 -DENABLE_PARQUET=1
fi

for flag in USE_AVRO USE_AWS_S3 USE_PARQUET; do
    grep -q "#define $flag 1" "$BUILD_DIR/includes/configs/config.h" \
        || { echo "ERROR: $flag is not enabled, Iceberg would be compiled out"; exit 1; }
done

rc=0
timeout --signal=INT "${BUDGET_MIN}m" ninja -C "$BUILD_DIR" -j"$(nproc)" clickhouse-bundle || rc=$?
sccache --show-stats || true
chmod -R a+rX "$SCCACHE_DIR" || true

if [ "$rc" -eq 124 ] || [ "$rc" -eq 130 ]; then
    echo "Time budget of ${BUDGET_MIN} min spent, the next job continues from the compiler cache"
    exit 124
fi
exit "$rc"
