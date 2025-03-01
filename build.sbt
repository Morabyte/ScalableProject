ThisBuild / version := "0.1"
ThisBuild / scalaVersion := "2.13.16"

lazy val root = (project in file("."))
  .settings(
    name := "CoPurchaseAnalysis"
  )

Compile / mainClass := Some("copurchase.analysis.Main")

//to avoid: java.lang.IllegalAccessError: class org.apache.spark.storage.StorageUtils$ (in unnamed module @0x44629c20) cannot access class sun.nio.ch.DirectBuffer (in module java.base) because module java.base does not export sun.nio.ch to unnamed module @0x44629c20
//la classe sun.nio.ch.DirectBuffer, che non è esportata dal modulo java.base. Questo problema è comune con Java 16+ a causa delle restrizioni sui moduli.
ThisBuild / fork := true
ThisBuild / javaOptions += "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
ThisBuild / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.3",
  "org.apache.spark" %% "spark-sql" % "3.5.2",

  "com.github.mrpowers" %% "spark-daria" % "1.2.3", //lib utils to write csv
)

//  sbt -J--add-exports=java.base/sun.nio.ch=ALL-UNNAMED run