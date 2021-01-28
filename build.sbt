val Http4sVersion = "0.21.15"
val CirceVersion = "0.13.0"
val MunitVersion = "0.7.20"
val LogbackVersion = "1.2.3"
val MunitCatsEffectVersion = "0.12.0"

lazy val root = (project in file("."))
  .settings(
    organization := "andy42",
    name := "ssc-programming-assignment2",
    version := "0.0.1-SNAPSHOT",
    scalaVersion := "2.13.4",
    libraryDependencies ++= Seq(
      "org.http4s" %% "http4s-blaze-server" % Http4sVersion,
      "org.http4s" %% "http4s-blaze-client" % Http4sVersion,
      "org.http4s" %% "http4s-circe" % Http4sVersion,
      "org.http4s" %% "http4s-dsl" % Http4sVersion,
      "io.circe" %% "circe-generic" % CirceVersion,
      "org.scalameta" %% "munit" % MunitVersion % Test,
      "org.typelevel" %% "munit-cats-effect-2" % MunitCatsEffectVersion % Test,
      "ch.qos.logback" % "logback-classic" % LogbackVersion,
      "org.scalameta" %% "svm-subs" % "20.2.0",

      "com.typesafe" % "config" % "1.4.0",

      "com.twitter.twittertext" % "twitter-text" % "3.1.0",

      "io.dropwizard.metrics" % "metrics-core" % "4.1.17",

      "org.scalactic" %% "scalactic" % "3.2.2" % Test,
      "org.scalatest" %% "scalatest" % "3.2.2" % Test,
      "io.circe" %% "circe-parser" % CirceVersion % Test
    ),
    addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.10.3"),
    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"),
    testFrameworks += new TestFramework("munit.Framework"),
    mainClass in (Compile, run) := Some("andy42.ssc.TWStreamApp")
  )
