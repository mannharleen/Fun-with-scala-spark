name := "IdeaProject_proj"

version := "1.0"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

scalaVersion := "2.10.6"


assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}

libraryDependencies ++= Seq(
	"org.apache.spark" % "spark-core_2.10" % "1.6.2",// % "provided",
	"org.apache.spark" % "spark-sql_2.10" % "1.6.2",//  % "provided",
	"org.apache.spark" % "spark-hive_2.10" % "1.6.2",// % "provided"
	"com.typesafe" % "config" % "1.3.1"

	)
