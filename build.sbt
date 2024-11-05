val org = "org.apache.pekko"
val libVersion = "0.1.0-SNAPSHOT"
val pekkoVersion = "1.1.2-uigc-SNAPSHOT"

ThisBuild / scalaVersion     := "2.13.15"
ThisBuild / version          := libVersion
ThisBuild / organization     := org

lazy val bench = (project in file("."))
  .settings(
    name := "uigc-bench",

    libraryDependencies ++= Seq(
      "org.apache.pekko" %% "pekko-actor-typed" % pekkoVersion,
      "org.apache.pekko" %% "pekko-actor-testkit-typed" % pekkoVersion,
      "org.apache.pekko" %% "pekko-cluster-typed" % pekkoVersion,
      "org.apache.pekko" %% "pekko-serialization-jackson" % pekkoVersion,
      "org.apache.pekko" %% "pekko-uigc" % pekkoVersion,
      "org.apache.pekko" %% "pekko-slf4j" % pekkoVersion,
      "ch.qos.logback" % "logback-classic" % "1.3.14",
    ),
    scalacOptions in Compile ++= Seq(
      "-optimise",
      "-Xdisable-assertions"
    ),

    resolvers += "Artima Maven Repository" at "https://repo.artima.com/releases",

    assemblyMergeStrategy in assembly := {
      case x if x.endsWith("module-info.class") => MergeStrategy.discard
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )
