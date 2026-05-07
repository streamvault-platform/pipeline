# ── Build stage ────────────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jdk-jammy AS builder
WORKDIR /build

RUN curl -fsSL "https://github.com/sbt/sbt/releases/download/v1.11.0/sbt-1.11.0.tgz" \
    | tar xz -C /usr/local
ENV PATH="/usr/local/sbt/bin:$PATH"

# Resolve deps before copying src — layer is cached unless build files change
COPY project/ project/
COPY build.sbt .
RUN sbt update

COPY src/ src/
RUN sbt assembly

# ── Runtime stage ──────────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jre-jammy AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Non-root user; gid=0 for OpenShift arbitrary-uid compatibility
RUN useradd -r -u 1001 -g root pipeline
WORKDIR /app

COPY --from=builder /build/target/scala-3.8.3/app.jar app.jar

USER pipeline

# All config comes from env at runtime (docker-compose / Helm values)
# See application.conf for local-dev defaults (localhost:9092)

EXPOSE 8080

ENTRYPOINT ["java", \
    "-XX:MaxRAMPercentage=75.0", \
    "-Djava.security.egd=file:/dev/./urandom", \
    "-jar", "app.jar"]
