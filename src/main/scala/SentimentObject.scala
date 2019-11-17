import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier, LogisticRegression, OneVsRest, RandomForestClassifier}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.feature.{HashingTF, IndexToString, StringIndexer, Tokenizer, VectorIndexer, Word2Vec}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}

import scala.io.Source
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StringType, StructField}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import TextUtilities.cleanText
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
//import lrModel.train_lr

object SentimentObject {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("DC")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
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

    val tweets = df.withColumnRenamed("Sentiment", "label")
    //    val Array(tweets_train, tweets_test) = df.randomSplit(Array(0.7, 0.3))
    val tweets_list = tweets.select("SentimentText").rdd.map(r => cleanText(r(0).toString)).collect()
    val new_column = tweets_list
    val rows = tweets.rdd.zipWithIndex.map(_.swap)
      .join(sc.parallelize(new_column).zipWithIndex.map(_.swap))
      .values
      .map { case (row: Row, x: String) => Row.fromSeq(row.toSeq :+ x) }
    val cleaned_tweets = sqlContext.createDataFrame(rows, tweets.schema.add("CleanedSentimentText", StringType, false))

    val lrTokenizer = new Tokenizer().setInputCol("SentimentText").setOutputCol("tokens")
    //      .setInputCol("cleaned")

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

    val lrCVmodel = cv.fit(cleaned_tweets)
    println(s"I kinda fitted the model")
    lrCVmodel.write.overwrite().save("/tmp/models/lrCVmodel")
    //    val sameModel = PipelineModel.load("/tmp/lrmodel")
    println("And now... Im gonna test the model!")
    val lrcvPredictions = lrCVmodel.transform(cleaned_tweets)
    val lrEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val lrcvAccuracy = lrEvaluator.evaluate(lrcvPredictions)
    println(s"LR CV with TFIDF train accuracy = ${lrcvAccuracy}")

//    //    lrCVmodel.clearThreshold
//    // Compute raw scores on the test set
//    val predictionAndLabels = cleaned_tweets.map { case LabeledPoint(label, features) =>
//      val prediction = lrCVmodel.predict(features)
//      (prediction, label)
//    }

    //Random Forest CV Classifier
    // Processing
    val rfTokenizer = new Tokenizer().setInputCol("SentimentText").setOutputCol("tokens")
    val rfHashingTF = new HashingTF()
      .setInputCol("tokens").setOutputCol("features")
      .setNumFeatures(100)
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(tweets)
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
    val rfcvModel = rfcv.fit(cleaned_tweets)
    rfcvModel.write.overwrite().save("/tmp/models/rfcvModel")
    val rfcvPredictions = rfcvModel.transform(cleaned_tweets)

    val rfcvEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val rfcvEvaluator2 = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("F-measure")
    val rfcvAccuracy = rfcvEvaluator.evaluate(rfcvPredictions)
    val rfcvFscore = rfcvEvaluator2.evaluate(rfcvPredictions)
    println(s"RFCV with TFIDF train accuracy = ${rfcvAccuracy}")
    println(s"RFCV with TFIDF train F-measure = ${rfcvFscore}")

  }

}