
name := "DBTF"

version := "2.0"

scalaVersion := "2.11.11"

val sparkVersion = "2.2.0"

lazy val providedSparkAndHadoopDependencies = Seq(
  ("org.apache.spark" %% "spark-core" % sparkVersion).
    exclude("org.mortbay.jetty", "servlet-api").
    exclude("commons-beanutils", "commons-beanutils-core").
    exclude("commons-collections", "commons-collections").
    exclude("commons-logging", "commons-logging").
    exclude("com.esotericsoftware.minlog", "minlog"),
  "org.apache.hadoop" % "hadoop-client" % "2.7.4" exclude("javax.servlet.jsp", "jsp-api") exclude("com.sun.jersey", "jersey-server") exclude("com.sun.jersey", "jersey-core") exclude("javax.servlet", "servlet-api") exclude("tomcat", "jasper-compiler") exclude("tomcat", "jasper-runtime")
  //"org.apache.hadoop" % "hadoop-client" % "2.7.0" excludeAll ExclusionRule(organization = "javax.servlet")
)
//libraryDependencies ++= providedSparkAndHadoopDependencies.map(_ % "provided")
libraryDependencies ++= providedSparkAndHadoopDependencies
//libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
//libraryDependencies += "org.apache.spark" %% "spark-catalyst" % sparkVersion
//libraryDependencies += "org.apache.spark" %% "spark-graphx" % sparkVersion
//libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion
libraryDependencies += "com.github.scopt" %% "scopt" % "3.5.0"
libraryDependencies += "commons-io" % "commons-io" % "2.5"

libraryDependencies += "org.scalactic" %% "scalactic" % "2.2.6"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.16"
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.16"
libraryDependencies += "org.clapper" %% "grizzled-slf4j" % "1.0.4"

scalacOptions ++= Seq("-deprecation", "-unchecked", "-Xlint", "-Xdisable-assertions")

// mainClass in assembly := Some("dbtf.DBTFDriver")
assemblyMergeStrategy in assembly := {
  case "features.xml" => MergeStrategy.first // infinispan-core
  case PathList("META-INF", "DEPENDENCIES.txt") => MergeStrategy.first // infinispan-core
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("org", "aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case PathList("org", "slf4j", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

test in assembly := {}

//assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

//run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))
