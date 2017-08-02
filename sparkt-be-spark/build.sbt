name := "sparkT-spark"

version := "0.1"
scalaVersion := "2.11.8"

// Spark
val sparkVersion = "2.0.2"
resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)
resolvers += Resolver.sonatypeRepo("snapshots")
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion
)

// Websocket server
libraryDependencies ++= {
  val akkaHttpVersion = "2.0.5"

  Seq(
    "com.typesafe.akka" %% "akka-stream-experimental" % akkaHttpVersion,
    "com.typesafe.akka" %% "akka-http-core-experimental" % akkaHttpVersion,
    "com.typesafe.akka" %% "akka-http-experimental" % akkaHttpVersion,
    "org.java-websocket" % "Java-WebSocket" % "1.3.0",
    "io.spray"          %%  "spray-json" % "1.3.3"
  )
}
