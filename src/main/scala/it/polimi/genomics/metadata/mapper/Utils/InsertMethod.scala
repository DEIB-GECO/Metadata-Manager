package it.polimi.genomics.metadata.mapper.Utils

import java.lang.NumberFormatException
import java.util
import collection.JavaConverters._
import com.typesafe.config.ConfigFactory
import it.polimi.genomics.metadata.Program.logger
import it.polimi.genomics.metadata.mapper.Predefined
import org.apache.commons.io.filefilter.FalseFileFilter
import org.apache.log4j.Logger
import org.openqa.selenium.{By, WebElement}
import org.openqa.selenium.htmlunit.HtmlUnitDriver
import scala.collection.mutable

object InsertMethod {
  private val logger: Logger = Logger.getLogger(this.getClass)
  private val conf = ConfigFactory.load()
  private val characterSeparation = conf.getString("import.method_character_separation")

  def selectInsertionMethod(sourceKey: String, globalKey: String, methods: String, concatCharacter: String, subCharacter: String, newCharacter: String, remCharacter: String) = (actualParam: String, newParam: String) => {
    methods.split("-").foldLeft(newParam) { case (acc, method) => {
      method.toUpperCase() match {
        case "DEFAULT" => acc
        case "EXTRACTCASES" => {
          var count = 0
          if (acc.contains("cases")) {
            val tmp = acc.split(", ")
            for (i <- tmp) {
              if (i.contains("cases")) {
                val check = i.split(" ")(0)
                //check if the field starts with "UP ..." or "UP TO ..."
                if (check.toUpperCase().equals("UP")){ // UP 1234 cases
                  if (!i.split(" ")(1).toUpperCase().equals("TO")){
                    try {
                      count += i.split(" ")(1).replaceAll(",", "").replaceAll("\\.", "").toInt
                    }
                    catch {
                      case e: NumberFormatException => count += 0
                      case _: Throwable => count += 0
                    }
                  } else { // UP TO 1234 cases
                    try {
                      count += i.split(" ")(2).replaceAll(",", "").replaceAll("\\.", "").toInt
                    }
                    catch {
                      case e: NumberFormatException => count += 0
                      case _: Throwable => count += 0
                    }
                  }
                } else if (check.equals("")){ //check if the field contains an empty space
                  try{
                    count += i.split(" ")(1).replaceAll(",", "").replaceAll("\\.", "").toInt
                  }
                  catch {
                    case e: NumberFormatException => count += 0
                    case _: Throwable => count += 0
                  }
                } else if(check.charAt(0).isDigit){
                  try{
                    count += i.split(" ")(0).replaceAll(",", "").replaceAll("\\.", "").toInt
                  }
                  catch {
                    case e: NumberFormatException => count += 0
                    case _: Throwable => count += 0
                  }
                }
              }
            }
          }
          count.toString
        }
        case "EXTRACTCONTROLS" => {
          var count = 0
          if (acc.contains("controls")) {
            val tmp = acc.split(", ")
            for (i <- tmp) {
              if (i.contains("controls")) {
                val check = i.split(" ")(0)
                //check if the field starts with "UP ..." or "UP TO ..."
                if (check.toUpperCase().equals("UP")){ // UP 1234 cases
                  if (!i.split(" ")(1).toUpperCase().equals("TO")){
                    try {
                      count += i.split(" ")(1).replaceAll(",", "").replaceAll("\\.", "").toInt
                    }
                    catch {
                      case e: NumberFormatException => count += 0
                      case _: Throwable => count += 0
                    }
                  } else { // UP TO 1234 cases
                    try {
                      count += i.split(" ")(2).replaceAll(",", "").replaceAll("\\.", "").toInt
                    }
                    catch {
                      case e: NumberFormatException => count += 0
                      case _: Throwable => count += 0
                    }
                  }
                } else if (check.equals("")){ //check if the field contains an empty space
                  try{
                    count += i.split(" ")(1).replaceAll(",", "").replaceAll("\\.", "").toInt
                  }
                  catch {
                    case e: NumberFormatException => count += 0
                    case _: Throwable => count += 0
                  }
                } else if(check.charAt(0).isDigit){
                  try{
                    count += i.split(" ")(0).replaceAll(",", "").replaceAll("\\.", "").toInt
                  }
                  catch {
                    case e: NumberFormatException => count += 0
                    case _: Throwable => count += 0
                  }
                }
              }
            }
          }
          count.toString
        }
        case "EXTRACTINDIVIDUALS" => {
          var count = 0
          if (acc.contains("individuals") || acc.contains("men") || acc.contains("women")){
            val tmp = acc.split(", ")
            for (i <- tmp) {
              if (i.contains("individuals") || i.contains("men") ||i.contains("women")){
                val check = i.split(" ")(0)
                //check if the field starts with "UP ..." or "UP TO ..."
                if (check.toUpperCase().equals("UP")){ // UP 1234 cases
                  if (!i.split(" ")(1).toUpperCase().equals("TO")){
                    try {
                      count += i.split(" ")(1).replaceAll(",", "").replaceAll("\\.", "").toInt
                    }
                    catch {
                      case e: NumberFormatException => count += 0
                      case _: Throwable => count += 0
                    }
                  } else { // UP TO 1234 cases
                    try {
                      count += i.split(" ")(2).replaceAll(",", "").replaceAll("\\.", "").toInt
                    }
                    catch {
                      case e: NumberFormatException => count += 0
                      case _: Throwable => count += 0
                    }
                  }
                } else if (check.equals("")){ //check if the field contains an empty space
                    try{
                      count += i.split(" ")(1).replaceAll(",", "").replaceAll("\\.", "").toInt
                    }
                    catch {
                      case e: NumberFormatException => count += 0
                      case _: Throwable => count += 0
                    }
                } else if(check.charAt(0).isDigit){
                  try{
                    count += i.split(" ")(0).replaceAll(",", "").replaceAll("\\.", "").toInt
                  }
                  catch {
                    case e: NumberFormatException => count += 0
                    case _: Throwable => count += 0
                  }
                }
              }
            }
          }
          count.toString
        }
        case "EXTRACTTRIOS" => {
          var count = 0
          if (acc.contains("trios")) {
            val tmp = acc.split(", ")
            for (i <- tmp) {
              if (i.contains("trios")) {
                val check = i.split(" ")(0)
                //check if the field starts with "UP ..." or "UP TO ..."
                if (check.toUpperCase().equals("UP")){ // UP 1234 cases
                  if (!i.split(" ")(1).toUpperCase().equals("TO")){
                    try {
                      count += i.split(" ")(1).replaceAll(",", "").replaceAll("\\.", "").toInt
                    }
                    catch {
                      case e: NumberFormatException => count += 0
                      case _: Throwable => count += 0
                    }
                  } else { // UP TO 1234 cases
                    try {
                      count += i.split(" ")(2).replaceAll(",", "").replaceAll("\\.", "").toInt
                    }
                    catch {
                      case e: NumberFormatException => count += 0
                      case _: Throwable => count += 0
                    }
                  }
                } else if (check.equals("")){ //check if the field contains an empty space
                  try{
                    count += i.split(" ")(1).replaceAll(",", "").replaceAll("\\.", "").toInt
                  }
                  catch {
                    case e: NumberFormatException => count += 0
                    case _: Throwable => count += 0
                  }
                } else if(check.charAt(0).isDigit){
                  try{
                    count += i.split(" ")(0).replaceAll(",", "").replaceAll("\\.", "").toInt
                  }
                  catch {
                    case e: NumberFormatException => count += 0
                    case _: Throwable => count += 0
                  }
                }
              }
            }
          }
          count.toString
        }
        case "MANUAL" => if (sourceKey == "null") null else sourceKey
        case "CONCAT" => if (actualParam == null) acc else actualParam.concat(concatCharacter + acc)
        case "CHECKPREC" => if (actualParam == null) acc else actualParam
        case "SELECTCASETCGA" => if (actualParam == null) getSourceSiteByCode(acc) else actualParam.concat(concatCharacter + getSourceSiteByCode(acc))
        case "SOURCEPAGEGDC" => acc.split(",").map(x => conf.getString("import.gdc_source_page") + x).mkString(",")
        case "REMOVE" => this.remove(remCharacter, newParam) //this is only used in TCGA2BED mapping
        case "SUB" => this.replace(subCharacter, newCharacter, acc)
        case "SPLIT" => acc.split(",").head
        case "UPPERCASE" => acc.toUpperCase()
        case "LOWERCASE" => acc.toLowerCase()
        case "DATETODAYS" => this.selectDayByParam(newParam)
        case "WEEKSTODAYS" => if (this.weeksToDays(newParam) != null) this.weeksToDays(newParam) else null
        case "ONTOLOGY" => sourceKey + '*' + acc
        case "PREDEFINED" => Predefined.map.get(sourceKey) match {
          case Some(x) =>
            x
          case None =>
            logger.error("Method PREDEFINED failed for sourceKey: " + sourceKey)
            actualParam
        }
        case "KGBIOSAMPLETYPE" => if (acc=="blood") "tissue" else if (acc=="lcl") "cell line" else null
        case "KGBIOSAMPLECELL" => if (acc=="lcl") "lymphoblastoid cell line" else null
        case "KGBIOSAMPLETISSUE" => if (acc=="blood") "blood" else null
        case _ => {
          logger.error("Method " + method + " not found");
          actualParam
        }
      }
    }
    }
  }

  private def substituteWordWith(phrase: String, wordToSub: String, wordToIns: String): String = {
    phrase.replace(wordToSub, wordToIns)
  }

  private def replaceAndConcat(actualParam: String, newParam: String, wordToSub: String, wordToIns: String, division: String): String = {
    if (actualParam == null) this.substituteWordWith(newParam, wordToSub, wordToIns) else actualParam.concat(division + this.substituteWordWith(newParam, wordToSub, wordToIns))
  }

  private def remove(remString: String, initialString: String): String = {
    remString.split(characterSeparation).foldLeft(initialString) { (acc, i) => acc.replace(i, "") }
  }

  private def replace(subString: String, newString: String, initialString: String): String = {
    var newStringArray = newString.split(characterSeparation)
    subString.split(characterSeparation).foldLeft(initialString) { (acc, i) => {
      val head = newStringArray.head
      newStringArray = newStringArray.drop(1)
      acc.replace(i, head)
    }
    }
  }

  private def selectDayByParam(param: String): String = {
    val values = param.split(" ")(0)
    param.split(" ")(1).toUpperCase match {
      case "YEAR"|"YEARS" => {
        (average(toArrayInt(values.split("-"))) * 365).toInt.toString
      }
      case "MONTH"|"MONTHS" => {
        (average(toArrayInt(values.split("-"))) * 30).toInt.toString
      }
      case "DAY"|"DAYS" => {
        average(toArrayInt(values.split("-"))).toInt.toString
      }
      case "WEEK"|"WEEKS" => {
        (average(toArrayInt(values.split("-"))) * 7).toInt.toString
      }
      case _ => {
        logger.error(s"Invalid argument Exception ${param.split(" ")(0)}")
        null
      }
    }
  }

  private def weeksToDays(param: String): String = {
    try {
      (param.toInt * 7).toString
    }
    catch {
      case e: NumberFormatException => {
        logger.error(s"NumberFormatException: ${param}")
        null
      }
    }
  }

  private def toArrayInt(list: Array[String]): Array[Float] = list.map(_.toFloat)

  private def average(list: Array[Float]): Float = list.sum / list.length

  private def getSourceSiteByCode(code: String): String = {
    try{
      val driver = new HtmlUnitDriver
      driver.get(conf.getString("import.tcga_tss_codes"))
      val peers: util.List[WebElement] = driver.findElementsByXPath("//table")
      val table: WebElement = peers.get(1)
      val tr_elements: mutable.Seq[WebElement] = table.findElements(By.xpath("//tr")).asScala

      val tss: mutable.Seq[WebElement] = tr_elements.slice(6, tr_elements.length)
      val tss_map = collection.mutable.Map[String, (String, String, String)]()

      for (e: WebElement <- tss) {
        val one = e.findElements(By.xpath("td")).asScala
        val tup = (one(0).getText, one(1).getText, one(2).getText, one(3).getText): Tuple4[String, String, String, String]
        tss_map += (tup._1 -> (tup._2, tup._3, tup._4))
      }

      tss_map.get(code.toUpperCase).get._1
    }
    catch{
      case e: Exception =>
        logger.warn("Something went wrong when retrieving tss code translation for TCGA.")
      code
    }


  }



}
