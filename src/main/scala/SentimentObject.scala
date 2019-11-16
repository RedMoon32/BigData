import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{LogisticRegression, OneVsRest}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.feature.{StringIndexer, Tokenizer}
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import scala.io.Source

object SentimentObject {
  def main(args: Array[String]): Unit = {
    var RegexList = Map[String, String]()
    RegexList += ("punctuation" -> "[^a-zA-Z0-9]")
    RegexList += ("digits" -> "\\b\\d+\\b")
    RegexList += ("white_space" -> "\\s+")
    RegexList += ("small_words" -> "\\b[a-zA-Z0-9]{1,2}\\b")
    RegexList += ("urls" -> "(https?\\://)\\S+")
    //    RegexList += ("aliases" -> "@\\b[a-zA-Z0-9]\\b")
    //todo: check regex for @...
    // todo: handle repeated letters as in "juuuust chilling!!"
    var Stopwords = Map[String, List[String]]()
    Stopwords += ("english" -> Source.fromFile("./data/stopwords.txt").getLines().toList)

    def removeRegex(txt: String, flag: String): String = {
      val regex = RegexList.get(flag)
      var cleaned = txt
      regex match {
        case Some(value) =>
          if (value.equals("white_space")) cleaned = txt.replaceAll(value, "")
          else cleaned = txt.replaceAll(value, " ")
        case None => println("No regex flag matched")
      }
      cleaned
    }

    def removeCustomWords(txt: String, flag: String): String = {
      var words = txt.split(" ")
      val stopwords = Stopwords.get(flag)
      stopwords match {
        case Some(value) => words = words.filter(x => !value.contains(x))
        case None => println("No stopword flag matched")
      }
      words.mkString(" ")
    }

    def cleanDocument(document_text: String): String = {
      var text = document_text.toLowerCase
      text = removeRegex(text, "urls")
      text = removeRegex(text, "punctuation")
      text = removeRegex(text, "digits")
      text = removeRegex(text, "small_words")
      text = removeRegex(text, "white_space")
      text = removeCustomWords(text, "english")
      text
    }


    val conf = new SparkConf().setMaster("local[2]").setAppName("DC")
    val sc = new SparkContext(conf)
    //    sc.setLogLevel("WARN")
    val sqlContext = new SQLContext(sc)
    val spark = org.apache.spark.sql.SparkSession
      .builder()
      .master("local[2]")
      .appName("Spark CSV Reader")
      .getOrCreate;
    import spark.implicits._

    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .load("./data/train.csv")
      .toDF("ItemID", "Sentiment", "SentimentText")

    val tweets = df.cache()
    val Array(trainingData, testData) = df.randomSplit(Array(0.7, 0.3))
    val tweets_train = trainingData.withColumnRenamed("Sentiment", "label")
    val tweets_test = testData.withColumnRenamed("Sentiment", "label")

    val lines = df.rdd
    val training_rdd = trainingData.rdd
    val test_rdd = testData.rdd

    tweets_train.printSchema()
    // Processing
    //    val indexer = new StringIndexer()
    //      .setInputCol("Sentiment")
    //      .setOutputCol("label")
    val tokenizer = new Tokenizer().setInputCol("SentimentText").setOutputCol("tokens")
    //      .setInputCol("cleaned")

    val hashingTF = new HashingTF()
      .setInputCol("tokens").setOutputCol("features")
      .setNumFeatures(100)

    // Classification
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.01)
    //    println(s"LogisticRegression parameters:\n ${lr.explainParams()}\n")
    val ovr = new OneVsRest()
      .setClassifier(lr)
    println("Pipeline")
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, lr))
    println("Im gonna fit a model you know")
    val model1 = pipeline.fit(tweets_train)
    println(s"Model 1 was fit using parameters: ${model1.parent.extractParamMap}")
    println(s"I kinda fitted the model")

    //    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
    // Now we can optionally save the fitted pipeline to disk
    model1.write.overwrite().save("/tmp/lrmodel")

    //    // And load it back in during production
    //    val sameModel = PipelineModel.load("/tmp/lrmodel")
    println("And now... Im gonna test the model!")
    model1.transform(tweets_test)
      .select("features", "label", "probability", "prediction")
      .collect()
      .foreach { case Row(features: Vector, label: Int, prob: Vector, prediction: Double) =>
        println(s"($features, $label) -> prob=$prob, prediction=$prediction")
      }
  }
}

