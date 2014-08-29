name := "svm orderbook dynamics"

version := "0.0.1"

organization := "com.scalafi.dynamics"


scalaVersion := "2.11.2"

scalacOptions += "-deprecation"

scalacOptions += "-feature"


// Resolvers

resolvers += "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases"

resolvers += "Scalafi Bintray Repo" at "http://dl.bintray.com/ezhulenev/releases"

// Library Dependencies

libraryDependencies ++= Seq(
  "org.scalaz"        %% "scalaz-core"     % "7.1.0",
  "org.scalaz.stream" %% "scalaz-stream"   % "0.5a",
  "com.scalafi"       %% "scala-openbook"  % "0.0.1"
)

// Test Dependencies

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest"   % "2.2.0" % "test"
)
