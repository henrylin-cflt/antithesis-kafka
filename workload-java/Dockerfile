FROM maven:3.9-eclipse-temurin-11 AS java-builder

WORKDIR /build

COPY pom.xml .

RUN mvn dependency:go-offline -B
COPY src ./src
RUN mvn clean package -DskipTests

COPY scripts/docker-entrypoint.sh ./scripts/docker-entrypoint.sh

FROM docker.io/rust:1.82 as rust-builder
WORKDIR /app
RUN apt-get update && apt-get install -y cmake
COPY ../workload/src /app/src
COPY Cargo.toml /app/Cargo.toml
# COPY Cargo.lock /app/Cargo.lock
RUN cargo install --locked --path .

FROM docker.io/debian:bookworm-slim
COPY --from=builder /usr/local/cargo/bin/workload /app/workload
COPY --from=builder /usr/local/cargo/bin/validation /app/validation

# Install useful utilities
RUN apt-get update && apt-get install -y \
    curl \
    netcat \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

#runtime
FROM eclipse-temurin:11-jre-jammy

# Copy built JARs from builder stage
COPY --from=java-builder /build/target/load.jar ./load.jar
COPY --from=java-builder /build/target/workload.jar ./workload.jar
COPY --from=java-builder /build/target/validation.jar ./validation.jar
COPY --from=java-builder /build/scripts/docker-entrypoint.sh ./docker-entrypoint.sh
COPY --from=rust-builder /usr/local/cargo/bin/validation /app/validation

# Create directories
RUN mkdir -p /app/logs /app/config

# Set environment variables
ENV JAVA_OPTS="-Xmx2g -Xms2g -XX:+UseG1GC -XX:MaxGCPauseMillis=100"
ENV LOG_DIR="/app/logs"

CMD ["./docker-entrypoint.sh"]