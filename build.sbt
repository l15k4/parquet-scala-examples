name := "parquet-scala-examples"

version := "1.0"

scalaVersion := "2.12.1"

lazy val parquetMrVersion = "1.8.1"
lazy val parquetFormatVersion = "2.3.1"
lazy val druidVersion = "0.9.2"

libraryDependencies ++= Seq(
  "org.apache.parquet" % "parquet-common"   % parquetMrVersion,
  "org.apache.parquet" % "parquet-hadoop"   % parquetMrVersion,
  "org.apache.parquet" % "parquet-column"   % parquetMrVersion,
  "org.apache.parquet" % "parquet-encoding" % parquetMrVersion,
  "org.apache.parquet" % "parquet-format"   % parquetFormatVersion,
  "org.apache.hadoop"  % "hadoop-client"    % "2.3.0",

  "org.slf4j"          % "slf4j-log4j12"    % "1.7.18",
  "org.slf4j"          % "slf4j-api"        % "1.7.18",
  "joda-time"          % "joda-time"        % "2.9.6"   % "test",
  "org.scalatest"      %% "scalatest"       % "3.0.0"   % "test",

  /* DRUID */

  "io.druid"                    % "druid-indexing-hadoop"     % druidVersion  % "test",
  "io.druid.extensions.contrib" % "druid-parquet-extensions"  % druidVersion  % "test",
  "io.druid.extensions"         % "druid-avro-extensions"     % druidVersion  % "test",
  "com.sun.jersey"              % "jersey-servlet"            % "1.19"        % "test"
)
