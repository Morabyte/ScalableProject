package copurchase.analysis

import org.apache.spark.sql.SparkSession
import java.time.Duration
import java.time.Instant
import org.apache.spark.rdd.RDD

object Main extends Serializable {
    val spark = SparkSession.builder()
    .appName("Co-Purchase Analysis")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  def main(args: Array[String]): Unit = {
    // Misura il tempo di esecuzione della versione ottimizzata
    CoPurchaseAnalysis.startAnalysis(Array("./resources/in/order_products.csv"))

    spark.stop()
    println("Finish")
  }
}