package copurchase.analysis

import java.time.Instant
import java.time.Duration
import spark.implicits._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import copurchase.analysis.Utily._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object Main extends Serializable {
  val spark = SparkSession.builder()
    .appName("Co-Purchase Analysis")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  val sc = spark.sparkContext

  spark.conf.set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
  spark.conf.set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
  //spark.conf.set("spark.hadoop.fs.gs.auth.service.account.json.keyfile", "<path>/serviceAccountkey.json") // -> to uncomment in local execution
  spark.conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
  spark.conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
  spark.conf.set("fs.gs.auth.service.account.enable", "true")
  
  def main(args: Array[String]): Unit = {
    var startTimeMillis: BigInt = 0
    var durationSeconds: BigInt = 0

    println("\tStart Analysis: ")
    startTimeMillis = System.currentTimeMillis()

    val inputPath = args(0) 

    val dataSet: RDD[(Int, Int)] = sc.textFile(inputPath)
      .map(line => {
        val cols = line.split(",")
        (cols(0).toInt, cols(1).toInt)
      })
      .persist() 
    
    println("\n\n\t\tRecord count: " + dataSet.count())

    val productPairs: RDD[((Int, Int), Int)] = dataSet
      .map { case (orderId, productId) => (orderId, List(productId)) }
      .reduceByKey(_ ++ _) 
      .flatMap { case (_, products) =>
        val distinctProducts = products.distinct 
        for {
          p1 <- distinctProducts
          p2 <- distinctProducts if p1 < p2 
        } yield ((p1, p2), 1)
      }

    val coPurchaseCounts = productPairs.reduceByKey(_ + _)

    val schema = new StructType(Array(
      StructField("product1", IntegerType, nullable = false),
      StructField("product2", IntegerType, nullable = false),
      StructField("count", IntegerType, nullable = false)
    ))
    
    Utily.writeToCSV(
      inputPath.split("/").dropRight(2).mkString("/") + "/out/",
      "coPurchaseAnalysis.csv",
      spark.sqlContext.createDataFrame( 
        coPurchaseCounts.sortBy({ case ((_, _), count) => count }, ascending = false)
          .map { case ((product1, product2), count) => Row(product1, product2, count) } 
        , schema 
      )
    )

    Utily.getSystemMetrics()
    durationSeconds = (System.currentTimeMillis() - startTimeMillis) / 1000
    println("\n\n\t\tThe programs time to be executed: " + (durationSeconds / 3600) + " hours " + ((durationSeconds / 60) % 60) + " minutes " + (durationSeconds % 60) + " seconds")

    spark.stop()
    println("Finish")
  }
}