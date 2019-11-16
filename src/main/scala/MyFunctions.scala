//import HelloWorld.RegexList
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object MyFunctions {
  var RegexList: Map[String, String] = Map[String, String]()
  // Building a List of Regex for PreProcessing the text
  //    var RegexList = Map[String, String]()
  RegexList += ("punctuation" -> "[^a-zA-Z0-9]")
  RegexList += ("digits" -> "\\b\\d+\\b")
  RegexList += ("white_space" -> "\\s+")
  RegexList += ("small_words" -> "\\b[a-zA-Z0-9]{1,2}\\b")
  RegexList += ("urls" -> "(https?\\://)\\S+")

  def removeRegex(txt: String, flag: String): String = {

    val regex = RegexList.get(flag)
    var cleaned = txt
    regex match {
      case Some(value) =>
        if (value.equals("white_space")) cleaned = txt.replaceAll(value, "")
        else cleaned = txt.replaceAll(value, " ")
      case None => println("No regex flag matched")
    }
    println("removed a regex, cleaned", cleaned)
    cleaned
  }

  //    def removeStuff(rdd: RDD[String], flag: String): RDD[String] = {
  //      val regex = this.RegexList.get(flag)
  //      var cleaned = rdd
  //      regex match {
  //        case Some(value) =>
  //          if (value.equals("white_space")) cleaned = sc.parallelize(List(rdd.toString().replaceAll(value, ""))).collect()
  //          else cleaned = sc.parallelize(List(rdd.toString().replaceAll(value, " "))).collect()
  //        case None => println("No regex flag matched")
  //      }
  //      println("removed a regex, cleaned", cleaned)
  //      cleaned
  //    }

//  def cleanDocument(rdd: RDD[Row], sc: SparkContext): RDD[Row]= {
//    var text = rdd.toString().toLowerCase
//    text = removeRegex(text, "urls")
//    println("i removed urls")
//    text = removeRegex(text, "punctuation")
//    text = removeRegex(text, "digits")
//    text = removeRegex(text, "small_words")
//    text = removeRegex(text, "white_space")
//    //    text = removeCustomWords(text, "english")
////    text
//    sc.parallelize(List(text)).collect()
//  }

//  // In the Scala API, DataFrame is simply a type alias of Dataset[Row]
//  def removeStuff(rdd: RDD[Row], sc: SparkContext): Unit = {
//    val flag = "punctuation"
//    val regex = RegexList.get(flag)
//    var cleaned = rdd
//    regex match {
//      case Some(value) =>
//        if (value.equals("white_space")) {
//          println(s"white spaces match")
//
//          cleaned = sc.parallelize(List(rdd.toString().replaceAll(value, ""))).collect()
//        }
//        else println(s"$value match") //cleaned = sc.parallelize(List(rdd.toString().replaceAll(value, " "))).collect()
//      case None => println("No regex flag matched")
//    }
//    //    println("removed a regex, cleaned", cleaned)
//    cleaned
//  }
}
