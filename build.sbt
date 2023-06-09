val org = "edu.illinois.osl"
val libVersion = "0.1.0-SNAPSHOT"
val akkaVersion = "2.8.0-M3+10-5847bc47-SNAPSHOT" // "2.6.3"

ThisBuild / scalaVersion     := "2.13.8"
ThisBuild / version          := libVersion
ThisBuild / organization     := org

lazy val bench = (project in file("."))
  .settings(
    name := "akka-gc-bench",

    libraryDependencies ++= Seq(
        org %% "akka-gc" % libVersion,
        "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
        "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion,
        "ch.qos.logback" % "logback-classic" % "1.2.3",
    )
  )
