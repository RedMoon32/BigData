import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.rdd.RDD

object Utiliies {

  def saveAsTextFileAndMerge[T](fileName: String, rdd: RDD[T]) = {
    val sourceFile = "./tmp/"
    rdd.saveAsTextFile(sourceFile)
    val dstPath = "./final/"
    merge(sourceFile, dstPath, fileName)
  }

  def merge(srcPath: String, dstPath: String, fileName: String): Unit = {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    val destinationPath = new Path(dstPath)
    if (!hdfs.exists(destinationPath)) {
      hdfs.mkdirs(destinationPath)
    }
    val filePath = new Path(dstPath + "/" + fileName)
    if (hdfs.exists(filePath)){
      hdfs.delete(filePath, false)
    }
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, filePath, true, hadoopConfig, null)
  }
}
