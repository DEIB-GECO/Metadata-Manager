package it.polimi.genomics.importer.ModelDatabase.Utils

import org.codehaus.jackson.{JsonNode, JsonParser, JsonToken}
import org.codehaus.jackson.map.MappingJsonFactory
import java.io.File

import it.polimi.genomics.importer.ModelDatabase.{DerivedFrom, Item}
import org.apache.log4j.Logger


class PlatformRetriver (val path: String, val originalSourceId: String){
  val logger: Logger = Logger.getLogger(this.getClass)
  private val replaceTransformation = "Transformations".r
  private val replaceExtensions = ".bed.meta".r
  println(path)
  val jsonFile = new File(replaceExtensions.replaceAllIn(replaceTransformation.replaceAllIn(path,"Downloads"),".bed.gz.json"))
  println(replaceExtensions.replaceAllIn(replaceTransformation.replaceAllIn(path,"Downloads"),".bed.gz.json"))
  val f = new MappingJsonFactory()
  val jp: JsonParser = f.createJsonParser(jsonFile)
  val rootNode: JsonNode = jp.readValueAsTree()
 // var platform : String = new String
  //var replicateId : String =_

  var containerId: Int = _
  //var finalItemId: Int = _

  def getItems(finalItemId:Int, containerId: Int): Unit ={

    this.containerId = containerId
    //this.replicateId = replicateId
    println(finalItemId)
    recursiveItems(path.split('/').last.split('.')(0), finalItemId)
  }

  private def recursiveItems(fileId: String, finalItemId: Int): Unit ={
   // println(fileId)
    if (rootNode.has("files")) {
      val files = rootNode.get("files").getElements
      while(files.hasNext) {
        val file = files.next()
        if (file.has("@id") && file.get("@id").asText().contains(fileId)) {
          if(file.has("derived_from")) {
            var initialItemId = 0
            if(file.get("accession").asText() != originalSourceId){
              println("Inserisci Item " + file.get("accession"))
              var item = new Item
              item.containerId = containerId
              item.sourceId = file.get("accession").asText()
              item.dataType = file.get("output_type").asText()
              item.format = file.get("file_type").asText()
              item.size = file.get("file_size").asInt()
              item.sourceUrl = file.get("href").asText()
              if(file.has("analysis_step_version")){
              val pipelines = file.findValue("pipelines").getElements
              while(pipelines.hasNext){
                var pipeline = pipelines.next()
                item.pipeline = item.pipeline.concat(" " + pipeline.get("title").asText())
              }
              }
              //println(file.findValues("pipelines").forEach(value => value.get("description")))
              if(item.checkInsert()){
                initialItemId = item.specialInsert()
                println("Item inserito " + initialItemId)
                var derivedFrom = new DerivedFrom
                derivedFrom.initialItemId = initialItemId
                derivedFrom.finalItemId = finalItemId
                if(file.has("analysis_step_version")) {
                  val analysis = file.findValue("analysis_step_types").getElements
                  println(analysis)
                  while (analysis.hasNext) {
                    var analys = analysis.next()
                    println(analys.asText())
                    derivedFrom.description = derivedFrom.description.concat(" " + analys.asText())
                  }
                }
                if(derivedFrom.checkInsert())
                  derivedFrom.insert()
                else
                  derivedFrom.update()
              }else{
                initialItemId = item.specialUpdate()
                println(item.sourceId)
                println("Item aggiornato " + initialItemId)
                var derivedFrom = new DerivedFrom
                derivedFrom.initialItemId = initialItemId
                derivedFrom.finalItemId = finalItemId
                if(file.has("analysis_step_version")) {
                  val analysis = file.findValue("analysis_step_types").getElements
                  println(analysis)
                  while (analysis.hasNext) {
                    var analys = analysis.next()
                    derivedFrom.description = derivedFrom.description.concat(" " + analys.asText())
                  }
                }
                //derivedFrom.description = file.findValue("analysis_step").findValue("analysis_step").get("analysis_step_types").asText()
                if(derivedFrom.checkInsert())
                  derivedFrom.insert()
                else
                  derivedFrom.update()
              }
            }
            val nextFiles = file.get("derived_from").getElements
            while (nextFiles.hasNext) {
              val nextFile = nextFiles.next()
              //println("Derived from: " + nextFile.asText().split("/")(2) + " in file " + file.get("@id").asText())
              if(file.get("accession").asText() == originalSourceId)
                recursiveItems(nextFile.asText().split("/")(2),finalItemId)
              else
                recursiveItems(nextFile.asText().split("/")(2),initialItemId)
            }
          }
          else {
           // println("Not Derived from " + file.findValue("accession").asText() + " in file " +file.get("@id").asText())
            if(file.get("accession").asText() != originalSourceId) {
              println("Inserisci Item " + file.get("accession"))
              var item = new Item
              var initialItemId = 0
              item.containerId = containerId
              item.sourceId = file.get("accession").asText()
              item.dataType = file.get("output_type").asText()
              item.format = file.get("file_type").asText()
              item.size = file.get("file_size").asInt()
              item.sourceUrl = file.get("href").asText()
              if(file.has("platform"))
                item.platform = file.findValue("platform").get("term_name").asText()
              if (item.checkInsert()) {
                initialItemId = item.specialInsert()
                println("Item inserito  " + initialItemId)
                var derivedFrom = new DerivedFrom
                derivedFrom.initialItemId = initialItemId
                derivedFrom.finalItemId = finalItemId
                if(file.has("analysis_step_version")) {
                  val analysis = file.findValue("analysis_step_types").getElements
                  println(analysis)
                  while (analysis.hasNext) {
                    var analys = analysis.next()
                    derivedFrom.description = derivedFrom.description.concat(" " + analys.asText())
                  }
                }
                  //derivedFrom.description = file.findValue("analysis_step").findValue("analysis_step").findValue("analysis_step_types").asText()
                if (derivedFrom.checkInsert())
                  derivedFrom.insert()
                else
                  derivedFrom.update()
              }
              else {
                initialItemId = item.specialUpdate()
                println("Item aggiornato " + initialItemId)
                var derivedFrom = new DerivedFrom
                derivedFrom.initialItemId = initialItemId
                derivedFrom.finalItemId = finalItemId
                //derivedFrom.description = file.findValue("analysis_step").findValue("analysis_step").findValue("analysis_step_types").asText()
                if (derivedFrom.checkInsert())
                  derivedFrom.insert()
                else
                  derivedFrom.update()
              }
            }
          }
        }

      }
    }

  }
}
