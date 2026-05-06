val zioVersion        = "2.1.26"
val zioConfigVersion  = "4.0.4"
val zioLoggingVersion = "2.5.0"
val fs2KafkaVersion   = "4.0.0"

lazy val root = (project in file("."))
  .settings(
    name         := "streamvault-pipeline",
    version      := "0.1.0-SNAPSHOT",
    scalaVersion := "3.8.3",
    libraryDependencies ++= Seq(
      "dev.zio"        %% "zio"                % zioVersion,
      "dev.zio"        %% "zio-streams"         % zioVersion,
      "dev.zio"        %% "zio-logging"         % zioLoggingVersion,
      "dev.zio"        %% "zio-config"          % zioConfigVersion,
      "dev.zio"        %% "zio-config-typesafe" % zioConfigVersion,
      "dev.zio"        %% "zio-config-magnolia" % zioConfigVersion,
      "dev.zio"        %% "zio-interop-cats"    % "23.1.0.13",
      "org.typelevel"  %% "fs2-kafka"           % fs2KafkaVersion,
      "dev.zio"        %% "zio-test"            % zioVersion % Test,
      "dev.zio"        %% "zio-test-sbt"        % zioVersion % Test,
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
    assembly / mainClass := Some("io.streamvault.pipeline.Main"),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case _                        => MergeStrategy.first
    }
  )
