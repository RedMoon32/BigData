import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import Utiliies.{getListOfSubDirectories, mergeModelOutput, saveAsTextFileAndMerge}

object FscoreChecker {

  def main(args: Array[String]): Unit = {

    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 2 cores to prevent from a starvation scenario.
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
      val a = lbl.compare(spark, "./data/labeled.csv",  "./final/models/"+i+"/output.csv")
      res = res:+((i,a.toDouble))
    }
    for (r <- res){
      println(s"F1 Score on  ${r._1} - ${r._2}");
    }
  }
}