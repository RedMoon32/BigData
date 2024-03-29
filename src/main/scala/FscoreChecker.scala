import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import Utiliies.{getListOfSubDirectories, mergeModelOutput, saveAsTextFileAndMerge}
import org.apache.log4j.{Level, Logger}

object FscoreChecker {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    val spark = org.apache.spark.sql.SparkSession
      .builder()
      .master("local[2]")
      .appName("Spark CSV Reader")
      .getOrCreate;
    val data = "./final/models";
    val lbl = new LabelTrainedChecker();
    val models = getListOfSubDirectories(data)
    var res : List[(String, Double)] = List()
    for ( i <- models ) {
      val a = lbl.compare(spark, "./data/labeled.csv",  "./final/models/"+i+"/output.csv", i)
      res = res:+((i,a.toDouble))
    }
    for (r <- res){
      println(s"F1 Score on  ${r._1} - ${r._2}");
    }
  }
}