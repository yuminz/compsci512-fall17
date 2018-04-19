name := "finalProject"

version := "1.0"

scalaVersion := "2.11.7"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-language:postfixOps")

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.3.12",
  "com.typesafe.akka" %% "akka-testkit" % "2.3.12",
  "org.scalatest" % "scalatest_2.11" % "3.0.1" % "test",
  "org.scalafx" %% "scalafx" % "8.0.144-R12"
)

fork in run := true
connectInput in run := true
