# Stage 1: Build environment
FROM ubuntu:24.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    git \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . .

RUN cmake -B build -S . -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build --config Release -j $(nproc)

# Stage 2: Minimal runtime image
FROM ubuntu:24.04 AS runtime

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    libstdc++6 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /data

COPY --from=builder /app/build/DistributedKVStore/kvstore-server /usr/local/bin/kvstore-server
COPY --from=builder /app/build/DistributedKVStore/kvstore-cli /usr/local/bin/kvstore-cli
COPY --from=builder /app/build/DistributedKVStore/kvstore-bench /usr/local/bin/kvstore-bench
COPY --from=builder /app/build/DistributedKVStore/kvstore-tests /usr/local/bin/kvstore-tests

EXPOSE 6379 7001

ENTRYPOINT ["kvstore-server"]
CMD ["--help"]