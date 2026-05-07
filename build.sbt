val zioVersion = "2.1.26"
val zioConfigVersion = "4.0.4"
val zioLoggingVersion = "2.5.0"
val fs2KafkaVersion = "4.0.0"

lazy val root = (project in file("."))
  .settings(
    name := "pipeline",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := "3.8.3",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % zioVersion,
      "dev.zio" %% "zio-streams" % zioVersion,
      "dev.zio" %% "zio-logging" % zioLoggingVersion,
      "dev.zio" %% "zio-config" % zioConfigVersion,
      "dev.zio" %% "zio-config-typesafe" % zioConfigVersion,
      "dev.zio" %% "zio-config-magnolia" % zioConfigVersion,
      "dev.zio" %% "zio-interop-cats" % "23.1.0.13",
      "org.typelevel" %% "fs2-kafka" % fs2KafkaVersion,
      "dev.zio" %% "zio-json" % "0.9.2",
      "dev.zio" %% "zio-http" % "3.11.1",
      "net.jthink" % "jaudiotagger" % "3.0.1",
      "dev.zio" %% "zio-process" % "0.8.0",
      "dev.zio" %% "zio-metrics-connectors-prometheus" % "2.5.5",
      "dev.zio" %% "zio-test" % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % zioVersion % Test,
      "org.testcontainers" % "kafka" % "1.21.4" % Test,
      "org.wiremock" % "wiremock-standalone" % "3.9.2" % Test
    ),

    fork := true,
    Test / javaOptions ++= Seq("-Djava.net.preferIPv4Stack=true"),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
    assembly / mainClass    := Some("io.streamvault.pipeline.Main"),
    assembly / assemblyJarName := "app.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case _                        => MergeStrategy.first
    }
  )

// unit-test  : fast, no Docker required (ffmpeg must be on PATH for TranscoderSpec)
// integration: needs Docker — Kafka via Testcontainers
addCommandAlias("unit-test",
  "testOnly io.streamvault.pipeline.domain.EventSerializationSpec " +
           "io.streamvault.pipeline.jobs.MetadataExtractorSpec " +
           "io.streamvault.pipeline.jobs.TranscoderSpec")
addCommandAlias("integration-test",
  "testOnly io.streamvault.pipeline.infra.EventProducerSpec " +
           "io.streamvault.pipeline.consumer.MediaUploadedConsumerIntegrationSpec")
