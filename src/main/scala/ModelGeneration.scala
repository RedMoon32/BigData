import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{LinearSVC, LogisticRegression, RandomForestClassifier}
import org.apache.spark.ml.feature.{HashingTF, IndexToString, StringIndexer, Tokenizer, VectorIndexer, Word2Vec}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import TextUtilities.cleanText
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

object ModelGeneration {
  val conf = new SparkConf().setMaster("local[2]").setAppName("DC")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    sc.setLogLevel("WARN")
    val spark = org.apache.spark.sql.SparkSession
      .builder()
      .master("local[2]")
      .appName("Spark CSV Reader")
      .getOrCreate;

    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .load("./data/train.csv")
      .toDF("ItemID", "Sentiment", "SentimentText")

    val tweets = df.withColumnRenamed("Sentiment", "label")
    val tweets_list = tweets.select("SentimentText").rdd.map(r => cleanText(r(0).toString)).collect()
    val new_column = tweets_list
    val rows = tweets.rdd.zipWithIndex.map(_.swap)
      .join(sc.parallelize(new_column).zipWithIndex.map(_.swap))
      .values
      .map { case (row: Row, x: String) => Row.fromSeq(row.toSeq :+ x) }
    var cleaned_tweets = sqlContext.createDataFrame(rows, tweets.schema.add("CleanedSentimentText", StringType, false))
    val cleaned_tweets_rdd = cleaned_tweets.rdd.map {
      case Row(id: Int, label: Int, text: String, cleaned: String) => Row(id, label, text, cleaned, cleaned.split(" "))
    }
    cleaned_tweets = sqlContext.createDataFrame(cleaned_tweets_rdd, cleaned_tweets.schema.add("SplittedText", ArrayType(StringType), false))

    generateLR(cleaned_tweets, spark)
    generateRandomForest(cleaned_tweets, spark)
    generateSVMW2V(cleaned_tweets, spark)
    generateSVMTFIDF(cleaned_tweets, spark)
  }

  def generateSVMTFIDF(train: DataFrame, spark: SparkSession) = {
    val lsvc = new LinearSVC()
      .setMaxIter(10)
      .setRegParam(0.1)
    val lsvcTokenizer = new Tokenizer().setInputCol("SentimentText").setOutputCol("tokens")
    val lsvcHashingTF = new HashingTF()
      .setInputCol("tokens").setOutputCol("features")
      .setNumFeatures(100)
    val lsvcPipeline = new Pipeline()
      .setStages(Array(lsvcTokenizer, lsvcHashingTF, lsvc))
    val lsvcModel = lsvcPipeline.fit(train)
    lsvcModel.write.overwrite().save("./models/svmTFIDFModel")
    val lsvcPredictions = lsvcModel.transform(train)


    val lb = new LabelTrainedChecker()
    lb.fscore(lsvcPredictions.toDF(), spark, "SVM with TFIDF")

    val lsvcEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val lsvcAccuracy = lsvcEvaluator.evaluate(lsvcPredictions)
    println(s"Linear SVM with TFIDF train accuracy = ${lsvcAccuracy}")
  }


  def generateWord2Vec(train: DataFrame) = {
    var processedTrainRDD = train.select("CleanedSentimentText").rdd.map {
      case Row(text: String) => Row(text.split(" "))
    }
    val schema = new StructType().add("SplittedText", ArrayType(StringType), false)
    val processedTrain = sqlContext.createDataFrame(processedTrainRDD, schema)

    val word2Vec = new Word2Vec()
      .setInputCol("SplittedText")
      .setOutputCol("features")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2Vec.fit(processedTrain)
    model
  }


  def generateSVMW2V(train: DataFrame, spark: SparkSession) = {
    val word2Vec = generateWord2Vec(train)
    val svm = new LinearSVC()
    val svmPipeline = new Pipeline()
      .setStages(Array(word2Vec, svm))
    println("Im gonna fit a model you know")

    val model = svmPipeline.fit(train)
    println(s"I kinda fitted the model")
    model.write.overwrite().save("./models/svmW2Vmodel")
    println("And now... Im gonna test the model!")
    val svmPredictions = model.transform(train)

    val lb = new LabelTrainedChecker()
    lb.fscore(svmPredictions.toDF(), spark, "SVM with W2V")

    val svmEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val svmAccuracy = svmEvaluator.evaluate(svmPredictions)
    println(s"SVM with Word2Vec train accuracy = ${svmAccuracy}")
    model
  }


  def generateLR(train: DataFrame, spark:SparkSession): CrossValidatorModel = {
    val lrTokenizer = new Tokenizer().setInputCol("SentimentText").setOutputCol("tokens")

    val lrHashingTF = new HashingTF()
      .setInputCol("tokens").setOutputCol("features")
      .setNumFeatures(100)

    // Classification
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.01)

    val lrPipeline = new Pipeline()
      .setStages(Array(lrTokenizer, lrHashingTF, lr))
    println("Im gonna fit a model you know")

    val lrParamGrid = new ParamGridBuilder()
      .addGrid(lrHashingTF.numFeatures, Array(10, 100, 1000))
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .build()

    val cv = new CrossValidator()
      .setEstimator(lrPipeline)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(lrParamGrid)
      .setNumFolds(2) // Use 3+ in practice
      .setParallelism(2) // Evaluate up to 2 parameter settings in parallel

    val lrCVmodel = cv.fit(train)
    println(s"I kinda fitted the model")
    lrCVmodel.write.overwrite().save("./models/lrCVmodel")
    println("And now... Im gonna test the model!")
    val lrcvPredictions = lrCVmodel.transform(train)

    val lb = new LabelTrainedChecker()
    lb.fscore(lrcvPredictions.toDF(), spark, "LR with TFIDF")

    val lrEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val lrcvAccuracy = lrEvaluator.evaluate(lrcvPredictions)
    println(s"LR CV with TFIDF train accuracy = ${lrcvAccuracy}")
    lrCVmodel
  }

  def generateRandomForest(train: DataFrame, spark: SparkSession): CrossValidatorModel = {
    // Processing
    val rfTokenizer = new Tokenizer().setInputCol("SentimentText").setOutputCol("tokens")
    val rfHashingTF = new HashingTF()
      .setInputCol("tokens").setOutputCol("features")
      .setNumFeatures(100)
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(train)
    // Train a RandomForest model.
    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("features")
    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val rfPipeline = new Pipeline()
      .setStages(Array(labelIndexer, rfTokenizer, rfHashingTF, rf, labelConverter))
    val rfParamGrid = new ParamGridBuilder()
      .addGrid(rfHashingTF.numFeatures, Array(10, 100, 1000))
      .addGrid(rf.impurity, Array("entropy", "gini"))
      .addGrid(rf.maxDepth, Array(5, 7))
      .build()
    val rfcv = new CrossValidator()
      .setEstimator(rfPipeline)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(rfParamGrid)
      .setNumFolds(2) // Use 3+ in practice
      .setParallelism(2)
    val rfcvModel = rfcv.fit(train)
    rfcvModel.write.overwrite().save("./models/rfcvModel")
    val rfcvPredictions = rfcvModel.transform(train)

    val lb = new LabelTrainedChecker()
    lb.fscore(rfcvPredictions.toDF(), spark, "RF with TFIDF")

    val rfcvEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val rfcvAccuracy = rfcvEvaluator.evaluate(rfcvPredictions)
    println(s"RFCV with TFIDF train accuracy = ${rfcvAccuracy}")

    rfcvModel
  }


}