val org = "edu.rice.habanero"
val libVersion = "0.1.0-SNAPSHOT"
val akkaVersion = "2.8.0-M3+12-8df90fee+20230720-1741-SNAPSHOT"// "2.6.3"

ThisBuild / scalaVersion     := "2.13.8"
ThisBuild / version          := libVersion
ThisBuild / organization     := org

lazy val lib = (project in file("."))
  .settings(
    name := "savina",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "edu.illinois.osl" %% "uigc" % "0.1.0-SNAPSHOT",
    ),
    scalacOptions in Compile ++= Seq(
      "-optimise", 
      "-Xdisable-assertions"
    )
  )
