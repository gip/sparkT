name := "sparkt-ast-scala"

version := "0.1"
scalaVersion := "2.11.8"

// Spark
val sparkVersion = "2.0.2"
resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)
resolvers += Resolver.sonatypeRepo("snapshots")
libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "org.scala-lang" % "scala-compiler" % scalaVersion.value
)
