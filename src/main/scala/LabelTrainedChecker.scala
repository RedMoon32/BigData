import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

class LabelTrainedChecker {

  def compare(spark: SparkSession, inputName: String, inputHdfsPath: String, name: String) = {

    // todo - preprocess using CleanDocument
    val labeled = spark.read
      .format("csv")
      .option("header", "false")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .load(inputName)
      .toDF("label", "SentimentText1")

    val sc = spark.sparkContext

    labeled.show()

    val predicted = spark.read
      .format("csv")
      .option("header", "false")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .load(inputHdfsPath)
      .toDF("Time", "SentimentText", "prediction")


    predicted.show()
    val joined = predicted.join(labeled, predicted("SentimentText").equalTo(labeled("SentimentText1")), "inner").select("SentimentText", "label", "prediction")
    joined.show()
    fscore(joined, spark, s"$name streaming")
  }

  def fscore(merged: DataFrame, spark: SparkSession, name: String): Float = {
    import spark.implicits._
    val tp = merged
      .where($"label" === 1 && $"prediction" === 1).count()
    val tn = merged
      .where($"label" === 0 && $"prediction" === 0).count()
    val fp = merged
      .where($"label" === 0 && $"prediction" === 1).count()
    val fn = merged
      .where($"label" === 1 && $"prediction" === 0).count()
    val precision = (tp.toFloat / (tp + fp))
    val recall = (tp.toFloat  / (tp + fn))
    val fscore = 2 * ((precision * recall).toFloat  / (precision + recall))
    println(s"$name statistics:")
    println(s"\tPrecision: $precision")
    println(s"\tRecall: $recall")
    println(s"\tF1 Score: $fscore")
    fscore
  }
}
