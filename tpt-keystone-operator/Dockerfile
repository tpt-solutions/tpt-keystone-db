FROM rust:1.82-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/tpt-keystone-operator /usr/local/bin/tpt-keystone-operator
ENTRYPOINT ["/usr/local/bin/tpt-keystone-operator"]
