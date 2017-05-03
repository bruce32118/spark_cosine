name := "spark_cosine"

version := "1.0"

scalaVersion := "2.11.8"


libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.0"  
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.0" 
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.1.0"
libraryDependencies += "com.typesafe" % "config" % "1.3.1"
libraryDependencies += "com.github.ben-manes.caffeine" % "caffeine" % "2.1.0"
libraryDependencies += "joda-time" % "joda-time" % "2.9.7"
libraryDependencies += "net.liftweb" % "lift-json_2.11" % "2.6.2"

fork := true

assemblyMergeStrategy in assembly := {
    case PathList(ps @ _*) if ps.last endsWith ".class"=> MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".properties"=> MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".xml"=> MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".thrift"=> MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".html"=> MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".java"=> MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".js"=> MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".css"=> MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".so"=> MergeStrategy.first
    case x =>
       val oldStrategy = (assemblyMergeStrategy in assembly).value
       oldStrategy(x)
}




