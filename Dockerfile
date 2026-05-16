# Build the fat JAR before building this image: sbt assembly
# ── Runtime image ──────────────────────────────────────────────────────────────
FROM eclipse-temurin:25-jre-jammy

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Non-root user; gid=0 for OpenShift arbitrary-uid compatibility
RUN useradd -r -u 1001 -g root pipeline
WORKDIR /app

COPY target/pipeline.jar app.jar

USER pipeline

# All config comes from env at runtime (docker-compose / Helm values)
# See application.conf for local-dev defaults (localhost:9092)

EXPOSE 8080

ENTRYPOINT ["java", \
    "-XX:MaxRAMPercentage=75.0", \
    "-Djava.security.egd=file:/dev/./urandom", \
    "-jar", "app.jar"]
