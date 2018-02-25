package it.polimi.genomics.importer.ModelDatabase.Utils

import it.polimi.genomics.importer.ModelDatabase.Encode.Utils.{BioSampleList, ReplicateList}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer
import scala.xml.{Node, XML}

class XMLReaderEncode(val path: String, val replicates: ReplicateList, val biosamples: BioSampleList, var states: collection.mutable.Map[String, String]) {
  private val xml = XML.loadFile(path)
  private val default: String = "DEFAULT"
  val logger: Logger = LoggerFactory.getLogger(this.getClass)


  private var list = List(((xml \\ "table" \ "mapping" \ "source_key").text), ((xml \\ "table" \ "mapping" \ "global_key").text), (xml \\ "@name"))

  private var operations = new ListBuffer[List[String]]()
  for (x <- xml \\ "table") {
    try {
      for(xi <- x \ "mapping") {
        if((x \ "@name").toString() != "REPLICATES" && (x \ "@name").toString() != "BIOSAMPLES" && (x \ "@name").toString() != "DONORS"){
          var app = new ListBuffer[String]()
          app += ((x \ "@name").toString())
          app += ((xi \ "source_key").text).replaceAll("X",biosamples.BiosampleList.head)
          app += ((xi \ "global_key").text)
          /*if((xi \ "@method").toString()!="")
            app += ((xi \ "@method").toString())
          else
            app += default */
          app += this.getMethod(xi)
          app += this.getConcatCharacter(xi)
          app += this.getSubCharacter(xi)
          app += this.getNewCharacter(xi)
          app += this.getRemCharacter(xi)

          operations += app.toList
        }else if((x \ "@name").toString() == "BIOSAMPLES" || (x \ "@name").toString() == "DONORS"){
          //var position: Int = 0
          //var arrayList = new Array[ListBuffer[List[String]]](biosamples.bioSampleQuantity)
          biosamples.BiosampleList.map(number =>{
            var app = new ListBuffer[String]()
            app += ((x \ "@name").toString())
            app += ((xi \ "source_key").text).replaceAll("X",number)
            app += ((xi \ "global_key").text)
            app += this.getMethod(xi)
            app += this.getConcatCharacter(xi)
            app += this.getSubCharacter(xi)
            app += this.getNewCharacter(xi)
            app += this.getRemCharacter(xi)

            //arrayList(position) += app.toList
            //position += 1
            operations += app.toList
          })
        }
        else{
          for( position <- 0 to replicates.UuidList.length-1){

            var app = new ListBuffer[String]()
            app += ((x \ "@name").toString())
            app += ((xi \ "source_key").text  + "__" + replicates.TechnicalReplicateNumberList(position)).replaceAll("X", replicates.BiologicalReplicateNumberList(position))
            app += ((xi \ "global_key").text)
            app += this.getMethod(xi)
            app += this.getConcatCharacter(xi)
            app += this.getSubCharacter(xi)
            app += this.getNewCharacter(xi)
            app += this.getRemCharacter(xi)

            if((xi \ "source_key").text == "replicates__X__uuid")
              states += ((xi \ "source_key").text  + "__" + replicates.TechnicalReplicateNumberList(position)).replaceAll("X", replicates.BiologicalReplicateNumberList(position)) -> replicates.UuidList(position)
            if((xi \ "source_key").text == "replicates__X__biological_replicate_number")
              states += ((xi \ "source_key").text  + "__" + replicates.TechnicalReplicateNumberList(position)).replaceAll("X", replicates.BiologicalReplicateNumberList(position)) -> replicates.BiologicalReplicateNumberList(position)
            if((xi \ "source_key").text == "replicates__X__technical_replicate_number")
              states += ((xi \ "source_key").text  + "__" + replicates.TechnicalReplicateNumberList(position)).replaceAll("X", replicates.BiologicalReplicateNumberList(position)) -> replicates.TechnicalReplicateNumberList(position)
            operations += app.toList
          }
        }
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
