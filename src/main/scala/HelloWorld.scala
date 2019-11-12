import org.apache.spark.{SparkConf, SparkContext}

object main {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
      setMaster("local[2]").
      setAppName("LearnScalaSpark")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val helloWorldString = "Hello World!"
    print(helloWorldString)

  }
}