package copurchase.analysis

import copurchase.analysis.Main.spark
import copurchase.analysis.Utily
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}


object CoPurchaseAnalysis extends Serializable {
  def startAnalysis(args: Array[String]): Unit = {
    import spark.implicits._

    val sc = spark.sparkContext
    var startTimeMillis: BigInt = 0
    var durationSeconds: BigInt = 0

    println("\tStart Analysis: ")
    startTimeMillis = System.currentTimeMillis()

    // passo link del mio bucket -> Leggere il dataset CSV
    val inputPath = args(0) //+ "/resources/in/order_products.csv"

    //Lettura del dataset come RDD
    val dataSet: RDD[(Int, Int)] = sc.textFile(inputPath)
      .map(line => {
        val cols = line.split(",")
        (cols(0).toInt, cols(1).toInt)
      })
      .persist() // Mantiene in memoria per evitare riletture
    println("\t\tRecord count: " + dataSet.count())

    val productPairs: RDD[((Int, Int), Int)] = dataSet
      .map { case (orderId, productId) => (orderId, List(productId)) }
      .reduceByKey(_ ++ _) // Combina direttamente gli elementi dello stesso ordine
      .flatMap { case (_, products) =>
        val distinctProducts = products.distinct // Rimuove duplicati
        for {
          p1 <- distinctProducts
          p2 <- distinctProducts if p1 < p2 // Evita coppie duplicate
        } yield ((p1, p2), 1)
      }

    //Uso di reduceByKey per contare le co-occorrenze
    val coPurchaseCounts = productPairs.reduceByKey(_ + _)

    //Misurazione dello shuffle size
    val shuffleSize = sc.getExecutorMemoryStatus
        .map(_._2._1).sum // Stima della memoria usata dagli esecutori
    println("\t\tShuffle Size: " + Utily.bytesToReadable(shuffleSize) + " bytes")

    //Metriche di Garbage Collection
    val gcTime = sc.getExecutorMemoryStatus
      .map(_._2._2).sum //valore ottenuto in microsecondi, divido per 1.000.000
    println("\t\tGarbage Collection time  to be executed: " + ((gcTime/1000000) / 3600) + " hours " + (((gcTime/1000000) / 60) % 60) + " minutes " + ((gcTime/1000000) % 60) + " seconds")

    // Salvare il risultato in formato CSV
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

    durationSeconds = (System.currentTimeMillis() - startTimeMillis) / 1000
    println("\t\tThe programs time to be executed: " + (durationSeconds / 3600) + " hours " + ((durationSeconds / 60) % 60) + " minutes " + (durationSeconds % 60) + " seconds")
  }
}