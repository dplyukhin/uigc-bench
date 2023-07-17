val org = "edu.illinois.osl"
val libVersion = "0.1.0-SNAPSHOT"
val akkaVersion = "0.0.0+26633-a0763208-SNAPSHOT" // "2.8.0-M3+11-a0763208-SNAPSHOT" // "2.6.3"

ThisBuild / scalaVersion     := "2.13.8"
ThisBuild / version          := libVersion
ThisBuild / organization     := org

lazy val bench = (project in file("."))
  .settings(
    name := "akka-gc-bench",

    libraryDependencies ++= Seq(
        org %% "akka-gc" % libVersion,
        "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
        "com.typesafe.akka" %% "akka-cluster-typed" % akkaVersion,
        "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion,
        "ch.qos.logback" % "logback-classic" % "1.2.3",
        "com.typesafe.akka" %% "akka-serialization-jackson" % akkaVersion,
    ),

    resolvers += "Artima Maven Repository" at "https://repo.artima.com/releases",

    assemblyMergeStrategy in assembly := {
      case x if x.endsWith("module-info.class") => MergeStrategy.discard
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )
