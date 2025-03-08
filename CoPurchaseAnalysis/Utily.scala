package copurchase.analysis

import org.apache.spark.TaskContext
import org.apache.spark.SparkContext
import copurchase.analysis.Main.sc
import org.apache.spark.sql.DataFrame
import java.lang.management.ManagementFactory
import org.apache.hadoop.fs.{FileSystem, Path}

object Utily extends Serializable {
  def writeToCSV(path: String, filename: String, data: DataFrame): Unit = {

    data.repartition(1).write.option("header", true).mode("overwrite").csv(path)
    
    /*val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.listStatus(new Path(path)).foreach { fileStatus =>
      if (fileStatus.getPath.getName.endsWith(".crc") || fileStatus.getPath.getName == "_SUCCESS") {
        fs.delete(fileStatus.getPath, false)
      } else if (fileStatus.getPath.getName.endsWith(".csv")) {
        fs.rename(fileStatus.getPath, new Path(path + filename))
      }
    }*/
  }

  def bytesToReadable(bytes: Long): String = {
    if (bytes < 1024) s"$bytes Bytes"
    else if (bytes < 1024.0 * 1024) f"${bytes / 1024.0}%.2f KB"
    else if (bytes < 1024.0 * 1024 * 1024) f"${bytes / (1024.0 * 1024)}%.2f MB"
    else if (bytes < 1024.0 * 1024 * 1024 * 1024) f"${bytes / (1024.0 * 1024 * 1024)}%.2f GB"
    else f"${bytes / (1024.0 * 1024 * 1024 * 1024)}%.2f TB"
  }

  def getSystemMetrics(): Unit = {
    val osBean = ManagementFactory.getOperatingSystemMXBean

    val systemLoadAvg = osBean.getSystemLoadAverage
    val availableProcessors = osBean.getAvailableProcessors
    val shuffleSize = sc.getExecutorMemoryStatus.map(_._2._1).sum 
    val gcTime = sc.getExecutorMemoryStatus.map(_._2._2).sum / 1000000 
    val uptimeSeconds = ManagementFactory.getRuntimeMXBean.getUptime / 1000  

    println("\t\tCPU Load Avg (1 min): " + systemLoadAvg)
    println("\t\tAvailable Processors: " + availableProcessors)
    println("\t\tShuffle Size: " + Utily.bytesToReadable(shuffleSize) + " bytes")
    println("\t\tUptime: " + (uptimeSeconds / 3600) + " hours " + ((uptimeSeconds / 60) % 60) + " minutes " + (uptimeSeconds % 60) + " seconds")
    println("\t\tGarbage Collector: " + (gcTime / 3600) + " hours " + ((gcTime / 60) % 60) + " minutes " + (gcTime % 60) + " seconds")
  }
}