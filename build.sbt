val org = "edu.rice.habanero"
val libVersion = "0.1.0-SNAPSHOT"
val pekkoVersion = "1.1.2-uigc-SNAPSHOT"

ThisBuild / scalaVersion     := "2.13.15"
ThisBuild / version          := libVersion
ThisBuild / organization     := org

lazy val lib = (project in file("."))
  .settings(
    name := "savina",
    libraryDependencies ++= Seq(
      "org.apache.pekko" %% "pekko-actor-typed" % pekkoVersion,
      "org.apache.pekko" %% "pekko-slf4j" % pekkoVersion,
      "org.apache.pekko" %% "pekko-uigc" % pekkoVersion,
      "ch.qos.logback" % "logback-classic" % "1.3.14",
    ),
    scalacOptions in Compile ++= Seq(
      "-optimise", 
      "-Xdisable-assertions"
    )
  )
