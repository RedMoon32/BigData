
import Utiliies.saveAsTextFileAndMerge
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Streaming {

    def main(args: Array[String]): Unit = {
        streamSaving()
    }

    def streamSaving() = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount")
        val ssc = new StreamingContext(conf, Seconds(10))
        val lines = ssc.socketTextStream("10.90.138.32", 8989)
        ssc.checkpoint("./src/checkpoints")
        wordCounter(lines, ssc.sparkContext)
        var count = 1
        lines.foreachRDD { rdd =>
            if (!rdd.isEmpty) {
                rdd.saveAsTextFile(s"./src/texts/text$count")
                count += 1
            }
        }
        lines.print()
        ssc.start()
        ssc.awaitTermination()
    }

    def saveTweets(lines: ReceiverInputDStream[String]) = {

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
        // TODO add check for no changes during last window. So no excess writes will be
        val top = runningCounts.transform { rdd =>
                rdd.sortBy(_._2)
            }
        top.print()

        top.foreachRDD { rdd =>
            if (!rdd.isEmpty) {
                saveAsTextFileAndMerge(s"word_counts", rdd)
            }
        }
    }
}
