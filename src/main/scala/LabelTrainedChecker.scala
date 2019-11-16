import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

class LabelTrainedChecker {

  def compare(spark: SparkSession, inputName: String, inputHdfsPath: String) = {

    // todo - preprocess using CleanDocument
    val labeled = spark.read
      .format("csv")
      .option("header", "false")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .load(inputName)
      .toDF("ItemID", "Sentiment1", "Twitter1")

    val sc = spark.sparkContext

    labeled.show()

    val predicted = spark.read
      .format("csv")
      .option("header", "false")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .load(inputHdfsPath)
      .toDF("Time", "Twitter2", "Sentiment2")


    predicted.show()
    val joined = predicted.join(labeled, predicted("Twitter2").equalTo(labeled("Twitter1")), "inner").selectExpr("Twitter1", "Sentiment1", "Sentiment2")
    joined.show()
    fscore(joined, spark)
  }

  def fscore(merged: DataFrame, spark: SparkSession): Float = {
    import spark.implicits._
    val tp = merged
      .where($"Sentiment1" === 1 && $"Sentiment2" === 1).count()
    val tn = merged
      .where($"Sentiment1" === 1 && $"Sentiment2" === 0).count()
    val fp = merged
      .where($"Sentiment1" === 0 && $"Sentiment2" === 1).count()
    val fn = merged
      .where($"Sentiment1" === 0 && $"Sentiment2" === 0).count()
    val precision = (tp.toFloat / (tp + fp))
    val recall = (tp.toFloat  / (tp + fn))
    val fscore = 2 * ((precision * recall).toFloat  / (precision + recall))

    fscore
  }
}
