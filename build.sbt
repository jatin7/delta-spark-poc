name := "delta-spark-poc"

version := "0.1"

scalaVersion := "2.12.12"

val sparkVersion = "2.4.6"

val deltaVersion = "0.6.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-yarn" % sparkVersion,
  "org.apache.spark" %% "spark-catalyst" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "io.delta" %% "delta-core" % deltaVersion
)

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.5"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"

assemblyMergeStrategy in assembly := {
  case "reference.conf" => MergeStrategy.concat
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", "services", xs@_*) => MergeStrategy.concat
  case PathList("META-INF", xs) if xs.endsWith(".SF") || xs.endsWith(".DSA") || xs.endsWith(".RSA") => MergeStrategy.discard
  case PathList("META-INF", xs@_*) => MergeStrategy.last
  case _ => MergeStrategy.last
}

test in assembly := {}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyJarName in assembly := s"${name.value}_${scalaBinaryVersion.value}-${version.value}.jar"

scalacOptions ++= Seq(
  "-encoding", "utf8"
)
