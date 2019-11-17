import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.rdd.RDD
import java.io._

import Streaming.spark
import org.apache.spark.sql.DataFrame

object Utiliies {
  val hadoopConfig = new Configuration()
  val hdfs = FileSystem.get(hadoopConfig)

  def saveAsTextFileAndMerge[T](fileName: String, rdd: RDD[T]) = {
    val sourceFile = "./tmp/" + fileName + "/"
    rdd.saveAsTextFile(sourceFile)
    val dstPath = "./final/"
    merge(sourceFile, dstPath, fileName)
  }

  def merge(srcPath: String, dstPath: String, fileName: String): Unit = {
    val destinationPath = new Path(dstPath)
    if (!hdfs.exists(destinationPath)) {
      hdfs.mkdirs(destinationPath)
    }
    val filePath = new Path(dstPath + "/" + fileName)
    if (hdfs.exists(filePath)){
      hdfs.delete(filePath, false)
    }
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, filePath, false, hadoopConfig, null)
  }

  def getListOfSubDirectories(directoryName: String): Array[String] = {
      (new File(directoryName))
          .listFiles
          .filter(_.isDirectory)
          .map(_.getName)
  }

  def mergeModelOutput(modelName: String, output: DataFrame) = {
      val finalPath = "./final/models/" + modelName + "/"
      val tmpPath = "./tmp/models/" + modelName + "/"
      if (!hdfs.exists( new Path(finalPath + "output.csv")) && !output.isEmpty) {
        output.write.csv(tmpPath + "output.csv")
        merge(tmpPath, finalPath , "output.csv")
      }
      else {
        val labeledFile = spark.read
          .format("csv")
          .option("header", "false")
          .option("delimiter", ",")
          .option("inferSchema", "true")
          .load(finalPath + "output.csv")

        var result = output

        if (!labeledFile.isEmpty) {
          val labeled = labeledFile.toDF("Time", "OriginalText", "prediction")
          labeled.show()
          result = result.union(labeled)
        }

        if (!result.isEmpty){
          result.write.mode("overwrite").csv(tmpPath + "output.csv/")
//          merge(tmpPath + "/" + modelName + "/output.csv", finalPath, "output.csv")
          merge(tmpPath + "output.csv/", finalPath, "output.csv")
        }
      }
  }
}
