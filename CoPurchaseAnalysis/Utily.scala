package copurchase.analysis

import com.github.mrpowers.spark.daria.sql.DariaWriters
import copurchase.analysis.Main.spark
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, Row}

object Utily extends Serializable {
  def writeToCSV(path: String, filename: String, data: DataFrame): Unit = {
    data.write.option("header", true).csv(path + filename)
    //data.coalesce(1).write.option("header", true).format("csv").mode("overwrite").save(path + filename)
    /*DariaWriters.writeSingleFile(
      df = data,
      format = "csv",
      sc = spark.sparkContext,
      tmpFolder = path + "tmp/",
      filename = path + "tmp/" + filename,
      saveMode = "overwrite"
    )

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.rename(new Path(path + "tmp/" + filename), new Path(path + filename))

    fs delete new Path(path + "tmp/")
    fs delete new Path(path + "." + filename + ".crc")*/
  }

  def bytesToReadable(bytes: Long): String = {
    if (bytes < 1024) s"$bytes Bytes"
    else if (bytes < 1024 * 1024) f"${bytes / 1024.0}%.2f KB"
    else if (bytes < 1024 * 1024 * 1024) f"${bytes / (1024.0 * 1024)}%.2f MB"
    else if (bytes < 1024 * 1024 * 1024 * 1024) f"${bytes / (1024.0 * 1024 * 1024)}%.2f GB"
    else f"${bytes / (1024.0 * 1024 * 1024 * 1024)}%.2f TB"
  }
}