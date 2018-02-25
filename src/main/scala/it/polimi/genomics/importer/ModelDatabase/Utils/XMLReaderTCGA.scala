package it.polimi.genomics.importer.ModelDatabase.Utils

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer
import scala.xml.{Node, XML}

class XMLReaderTCGA(val path: String) {

  private val xml = XML.loadFile(path)
  private val default: String = "DEFAULT"
  val logger: Logger = LoggerFactory.getLogger(this.getClass)


  private var list = List(((xml \\ "table" \ "mapping" \ "source_key").text), ((xml \\ "table" \ "mapping" \ "global_key").text), (xml \\ "@name"))

  private var operations = new ListBuffer[List[String]]()
  for (x <- xml \\ "table") {
    try {
      for(xi <- x \ "mapping") {
          var app = new ListBuffer[String]()
          app += ((x \ "@name").toString())
          app += ((xi \ "source_key").text)
          app += ((xi \ "global_key").text)
          app += this.getMethod(xi)
          app += this.getConcatCharacter(xi)
          app += this.getSubCharacter(xi)
          app += this.getNewCharacter(xi)
          app += this.getRemCharacter(xi)

        operations += app.toList
      }
    } catch {
      case e: Exception => logger.warn(s"Source Key ${((x \ "mapping" \ "source_key").text)} doesn't find for table ${((x \ "@name").toString())}")
    }
  }

  private val _operationsList = operations.toList

  def operationsList = _operationsList

  private def getMethod(xi: Node) : String = {
    if((xi \ "@method").toString()!="")
      (xi \ "@method").toString()
    else
      default
  }

  private def getConcatCharacter(xi: Node) : String = {
    if((xi \ "@concat_character").toString()!="")
      (xi \ "@concat_character").toString()
    else
      " "
  }

  private def getSubCharacter(xi: Node): String = {
    if((xi \ "@sub_character").toString()!="")
      (xi \ "@sub_character").toString()
    else
      ""
  }

  private def getNewCharacter(xi: Node): String = {
    if((xi \ "@new_character").toString()!="")
      (xi \ "@new_character").toString()
    else
      ""
  }

  private def getRemCharacter(xi: Node): String = {
    if((xi \ "@rem_character").toString()!="")
      (xi \ "@rem_character").toString()
    else
      ""
  }
}
