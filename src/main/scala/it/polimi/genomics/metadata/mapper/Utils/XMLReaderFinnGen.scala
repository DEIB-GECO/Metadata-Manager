package it.polimi.genomics.metadata.mapper.Utils

import it.polimi.genomics.metadata.mapper.GWAS.Utils.AncestryList
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer
import scala.xml.XML


class XMLReaderFinnGen(val path: String, val ancestries: AncestryList, var states: collection.mutable.Map[String, String]) {
  private val xml = XML.loadFile(path)

  private val default: String = "DEFAULT"
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val settingRetriver: XMLSettingRetriver = new XMLSettingRetriver(default)

  private var operations = new ListBuffer[List[String]]()
  for (x <- xml \\ "table") {
    try {
    for(xi <- x \ "mapping") {
        var app = new ListBuffer[String]()
        app += ((x \ "@name").toString())
        app += ((xi \ "source_key").text)
        app += ((xi \ "global_key").text)

        app += settingRetriver.getMethod(xi)
        app += settingRetriver.getConcatCharacter(xi)
        app += settingRetriver.getSubCharacter(xi)
        app += settingRetriver.getNewCharacter(xi)
        app += settingRetriver.getRemCharacter(xi)

        operations += app.toList
      }
    } catch {
      case e: Exception => logger.warn(s"Source Key ${((x \ "mapping" \ "source_key").text)} not found for table ${((x \ "@name").toString())}")
    }
  }

  private val _operationsList = operations.toList

  def operationsList = _operationsList
}
