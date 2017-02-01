//
// SBT Build File for User Frequency Summary.
//
// Copyright (C) 2016.  xAd Inc.  All Rights Reserved.
//
name := "xad-user-frequency"

version := "0.9.0"

scalaVersion := "2.10.4"


//---------------------
// Scala Dependencies
//---------------------
//libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.1"

//libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.3.1"

//libraryDependencies += "org.scalanlp" %% "breeze" % "0.11.2"

//libraryDependencies += "org.scalanlp" %% "breeze-natives" % "0.11.2"

//libraryDependencies += "org.scalanlp" %% "breeze-viz" % "0.11.2"

//libraryDependencies += "com.github.scopt" %% "scopt" % "3.3.0"

//libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.5" % "test"

// For DatesToMillis
//libraryDependencies += "org.scalaj" % "scalaj-time_2.10.2" % "0.7"


//---------------------
// Java Dependencies
//---------------------
// libraryDependencies += "com.github.nikita-volkov" % "sext" % "0.2.4"

//libraryDependencies += "joda-time" % "joda-time" % "2.8.2"

//libraryDependencies += "org.joda" % "joda-convert" % "1.7"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.6.0"

libraryDependencies += "org.apache.pig" % "pig" % "0.14.0"

libraryDependencies += "org.apache.avro" % "avro" % "1.7.5"

//libraryDependencies += "commons-cli" % "commons-cli" % "1.3.1"

//libraryDependencies += "org.yaml" % "snakeyaml" % "1.16"


// TMP
//libraryDependencies += "org.xerial.snappy" % "snappy-java" % "1.1.2.1"


//-------------------
// xAd Dependencies
//-------------------
libraryDependencies += "com.xad.spring" % "xad_common" % "1.10.17"

//libraryDependencies += "com.xad.kpi" % "xad-kpi-client" % "2.0.0"


//------------
// Resolvers
//------------
//resolvers ++= Seq(
//  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
//  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
//)

// scopt
// "Sonatype Public" at https://oss.sonatype.org/content/repositories/public
//resolvers += Resolver.sonatypeRepo("public")

// xAd S3 Maven
resolvers += "xAd Maven" at "s3://xad-mvn-repo/maven2"


//----------
// Main
//----------
//mainClass in Compile := Some("com.xad.floor.CPDisplay")


//----------
// Misc
//----------
retrieveManaged := true


