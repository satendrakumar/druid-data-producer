name := "druid-data-producer"

version := "1.0"

scalaVersion := "2.11.8"


libraryDependencies ++=
             Seq(
                   "io.druid" %% "tranquility-core" % "0.8.2",
                  "ch.qos.logback" % "logback-classic" % "1.0.13"
             )


