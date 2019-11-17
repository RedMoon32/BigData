import scala.io.Source

object TextUtilities {
  //    RegexList += ("aliases" -> "@\\b[a-zA-Z0-9]\\b")
  // todo: check regex for @...
  // todo: handle repeated letters as in "juuuust chilling!!"

  var RegexList = Map[String, String]()
  RegexList += ("quotes" -> "@[^\\s]+")
  RegexList += ("punctuation" -> "[^a-zA-Z0-9]")
  RegexList += ("digits" -> "\\b\\d+\\b")
  RegexList += ("white_space" -> "\\s+")
  RegexList += ("small_words" -> "\\b[a-zA-Z0-9]{1,2}\\b")
  RegexList += ("urls" -> "(https?\\://)\\S+")

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

  def cleanText(document_text: String): String = {
    var text = document_text.toLowerCase
    text = removeRegex(text, "quotes")
    text = removeRegex(text, "urls")
    text = removeRegex(text, "punctuation")
    text = removeRegex(text, "digits")
    text = removeRegex(text, "small_words")
    text = removeRegex(text, "white_space")
    text = removeCustomWords(text, "english")
    text
  }
}
