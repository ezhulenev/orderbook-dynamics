import AssemblyKeys._


name := "orderbook dynamics"

version := "0.0.1"

organization := "com.scalafi.dynamics"

scalaVersion := "2.10.4"

scalacOptions += "-deprecation"

scalacOptions += "-feature"

licenses in ThisBuild += ("MIT", url("http://opensource.org/licenses/MIT"))

net.virtualvoid.sbt.graph.Plugin.graphSettings

// Merge strategy

val applicationMergeStrategy: (String => MergeStrategy) => String => MergeStrategy =
  old => {
    case x if x.startsWith("META-INF/ECLIPSEF.RSA") => MergeStrategy.last
    case x if x.startsWith("META-INF/mailcap")      => MergeStrategy.last
    case x if x.endsWith  ("plugin.properties")     => MergeStrategy.last
    case x => old(x)
  }

// Load Assembly Settings

assemblySettings

// Assembly App

mainClass in assembly := Some("com.scalafi.dynamics.DecisionTreeDynamics")

jarName in assembly := "order-book-dynamics.jar"

mergeStrategy in assembly <<= (mergeStrategy in assembly)(applicationMergeStrategy)


// Resolvers

resolvers += "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases"

resolvers += "Scalafi Bintray Repo" at "http://dl.bintray.com/ezhulenev/releases"

resolvers += "Pellucid Bintray" at "http://dl.bintray.com/pellucid/maven"

// Library Dependencies

val SparkExclusionRules = Seq(
  ExclusionRule("commons-beanutils", "commons-beanutils-core"),
  ExclusionRule("commons-collections", "commons-collections"),
  ExclusionRule("commons-logging", "commons-logging"),
  ExclusionRule("org.slf4j", "slf4j-log4j12"),
  ExclusionRule("org.hamcrest", "hamcrest-core"),
  ExclusionRule("junit", "junit"),
  ExclusionRule("org.jboss.netty", "netty"),
  ExclusionRule("com.esotericsoftware.minlog", "minlog")
)


libraryDependencies ++= Seq(
    "org.slf4j"          % "slf4j-api"       % "1.7.7",
    "joda-time"          % "joda-time"       % "2.3",
    "org.joda"           % "joda-convert"    % "1.5",
    "com.github.scopt"  %% "scopt"           % "3.2.0",
    "ch.qos.logback"     % "logback-classic" % "1.1.2",
    "com.scalafi"       %% "scala-openbook"  % "0.0.6",
    "com.pellucid"      %% "framian"         % "0.3.1",
    "org.apache.spark"  %% "spark-core"      % "1.1.0" excludeAll(SparkExclusionRules:_*),
    "org.apache.spark"  %% "spark-mllib"     % "1.1.0" excludeAll(SparkExclusionRules:_*)
  )


// Test Dependencies

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest"   % "2.2.0" % "test"
)