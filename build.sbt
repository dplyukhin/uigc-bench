val org = "org.apache.pekko"
val libVersion = "0.1.0-SNAPSHOT"
val pekkoVersion = "1.1.2-uigc-SNAPSHOT"

ThisBuild / scalaVersion     := "2.13.15"
ThisBuild / version          := libVersion
ThisBuild / organization     := org

lazy val savina = project
  .settings(
    name := "savina",
    libraryDependencies ++= Seq(
      "org.apache.pekko" %% "pekko-actor-typed" % pekkoVersion,
      "org.apache.pekko" %% "pekko-slf4j" % pekkoVersion,
      "org.apache.pekko" %% "pekko-uigc" % pekkoVersion,
      "ch.qos.logback" % "logback-classic" % "1.3.14",
    ),
    Compile / scalacOptions ++= Seq(
      "-optimise", 
      "-Xdisable-assertions"
    )
  )

lazy val workers = project
  .settings(
    name := "workers",

    libraryDependencies ++= Seq(
      "org.apache.pekko" %% "pekko-actor-typed" % pekkoVersion,
      "org.apache.pekko" %% "pekko-actor-testkit-typed" % pekkoVersion,
      "org.apache.pekko" %% "pekko-cluster-typed" % pekkoVersion,
      "org.apache.pekko" %% "pekko-serialization-jackson" % pekkoVersion,
      "org.apache.pekko" %% "pekko-uigc" % pekkoVersion,
      "org.apache.pekko" %% "pekko-slf4j" % pekkoVersion,
      "ch.qos.logback" % "logback-classic" % "1.3.14",
    ),
    Compile / scalacOptions ++= Seq(
      "-optimise",
      "-Xdisable-assertions"
    )
  )
