package it.polimi.genomics.importer.ModelDatabase.Utils

import scala.collection.mutable.ListBuffer
import scala.xml.XML

class XMLReader(val path: String) {
  private val xml = XML.loadFile(path)

  private var list = List(((xml \\ "table" \ "mapping" \ "source_key").text), ((xml \\ "table" \ "mapping" \ "global_key").text), (xml \\ "@name"))

  private var operations = new ListBuffer[List[String]]()
  for (x <- xml \\ "table") {
    try {
      for(xi <- x \ "mapping") {
        var app = new ListBuffer[String]()
        app += ((x \ "@name").toString())
        app += ((xi \ "source_key").text)
        app += ((xi \ "global_key").text)
        operations += app.toList
      }
    } catch {
      case e: Exception => println("Source_key non trovata")
    }
  }

  private val _operationsList = operations.toList

  def operationsList = _operationsList

}
