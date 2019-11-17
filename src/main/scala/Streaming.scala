import java.time.LocalDateTime

import TextUtilities.cleanText
import Utiliies.{getListOfSubDirectories, mergeModelOutput, saveAsTextFileAndMerge}
import org.apache.spark.ml.{PipelineModel}

import scala.collection.mutable.Map
import org.apache.spark.{SparkContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.{DataFrame, SparkSession}

object Streaming {
    val spark: SparkSession =
        SparkSession
          .builder()
          .appName("Streaming")
          .config("spark.master", "local[*]")
          .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(10))
    val models = loadModels()


  def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.WARN)
        Logger.getLogger("akka").setLevel(Level.WARN)
        streamSaving()
    }

    def streamSaving() = {
        val lines = ssc.socketTextStream("10.90.138.32", 8989)
        ssc.checkpoint("./src/checkpoints")
        wordCounter(lines, ssc.sparkContext)
        predictAndWriteAll(lines)
        ssc.start()
        ssc.awaitTermination()
    }

    def predictAndWriteAll(lines: ReceiverInputDStream[String]) = {
        println("PREDICTING")
        val now = LocalDateTime.now()
        var tweet = ""
        var count = 1
        lines.foreachRDD{ rdd =>
          if (!rdd.isEmpty) {
            val df = rdd.map {
              case originalText: String => (originalText, cleanText(originalText), now.toString)
            }.toDF("OriginalText", "SentimentText", "Time")
            predictAndWrite(df, count)
            count += 1
          }
        }
        lines.print()
    }

   def predictAndWrite(data: DataFrame, count: Int): Any = {
     if ( data.head(1).isEmpty ) {
       return
     }

     for ((k, v) <- models) {
       var result = v.transform(data)
         .select("Time", "OriginalText", "prediction")
       result = result.withColumn("prediction", $"prediction" cast "Int" as "prediction")
       mergeModelOutput(k, result)
     }
   }

    def loadModels(): Map[String, PipelineModel] = {
        println("Loading models")
        val modelsDirs = getListOfSubDirectories("./models")
        var models = Map[String, PipelineModel]()
        for (dir <- modelsDirs) {
            models += dir -> PipelineModel.load("./models/" + dir)
        }
        models
    }

    def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
        val newCount = runningCount.getOrElse(0) + newValues.sum
        Some(newCount)
    }

    def wordCounter(lines: ReceiverInputDStream[String], sc: SparkContext) = {
        val words = lines.flatMap(_.split(" "))//.countByValue()
        val pairs = words.map(word => (word, 1))
        val wordCounts = pairs.reduceByKey(_ + _)
        val runningCounts = wordCounts.updateStateByKey[Int](updateFunction _)
        val top = runningCounts.transform { rdd =>
                rdd.sortBy(_._2)
            }

        top.foreachRDD { rdd =>
            if (!rdd.isEmpty) {
                saveAsTextFileAndMerge(s"word_counts", rdd)
            }
        }
    }
}
