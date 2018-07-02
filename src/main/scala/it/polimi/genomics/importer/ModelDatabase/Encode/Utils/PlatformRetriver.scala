//package it.polimi.genomics.importer.ModelDatabase.Encode.Utils
//
//import java.io.File
//
//import it.polimi.genomics.importer.ModelDatabase.Encode.EncodeTableId
//import it.polimi.genomics.importer.ModelDatabase.Encode.Table.{CaseItemEncode, DerivedFromEncode, ItemEncode, ReplicateItemEncode}
//import org.apache.log4j.Logger
//import org.codehaus.jackson.map.MappingJsonFactory
//import org.codehaus.jackson.{JsonNode, JsonParser}
//
//import scala.collection.mutable.ListBuffer
//
//
//class PlatformRetriver (val path: String, val originalSourceId: String,var encodesTableId: EncodeTableId){
//  val logger: Logger = Logger.getLogger(this.getClass)
//
//  private val replaceTransformation = "Transformations".r
//  private val replaceExtensions = ".bed.meta.json".r
//  private val replaceSourceId = (originalSourceId+"_O").r
//  val jsonFile = new File(replaceExtensions.replaceAllIn(replaceTransformation.replaceAllIn(path,"Downloads"),".bed.gz.json"))
//  val jsonFile_O = new File(replaceSourceId.replaceAllIn(replaceExtensions.replaceAllIn(replaceTransformation.replaceAllIn(path,"Downloads"),".bed.gz.json"), originalSourceId))
//
//  val f = new MappingJsonFactory()
//  var jp: JsonParser = _
//
//  if(jsonFile.exists()) {
//     jp = f.createJsonParser(jsonFile)
//  }
//  else {
//    jp = f.createJsonParser(jsonFile_O)
//  }
//
//  val rootNode: JsonNode = jp.readValueAsTree()
//  var description: String = _
//  var experimentTypeId: Int = _
//  var caseId: Int = _
//
//  def getItems(finalItemId:Int, experimentTypeId: Int, caseId: Int): Unit = {
//    this.experimentTypeId = experimentTypeId
//    this.caseId = caseId
//    recursiveItems(path.split('/').last.split('.')(0), finalItemId, "")
//  }
//
//  private def recursiveItems(fileId: String, finalItemId: Int, precDescription: String): Unit ={
//    if (rootNode.has("files")) {
//      val files = rootNode.get("files").getElements
//      while(files.hasNext) {
//        val file = files.next()
//        if (file.has("@id") && file.get("@id").asText().contains(fileId)) {
//          if(file.has("derived_from")) {
//            var initialItemId = 0
//            if(file.get("accession").asText() != originalSourceId){
//              description = null
//              val item = defineItem(file)
//              if (item.checkInsert()) {
//                initialItemId = item.specialInsert()
//              }
//              else {
//                initialItemId = item.specialUpdate()
//              }
//              insertCaseItem(initialItemId)
//              getReplicatesAndInsert(file,initialItemId)
//              val derivedFrom = defineDerivedFrom(file,initialItemId,finalItemId,precDescription)
//              insertOrUpdateDerivedFrom(derivedFrom)
//            }
//            else{
//              if(file.has("analysis_step_version")) {
//                /*val analysis = file.findValue("analysis_step_types").getElements
//                while (analysis.hasNext) {
//                  val analys = analysis.next()
//                  if(description == null)
//                    this.description = analys.asText()
//                  else
//                    this.description = this.description.concat(" " + analys.asText())
//                }*/
//                setDescription(file)
//              }
//            }
//            val nextFiles = file.get("derived_from").getElements
//            while (nextFiles.hasNext) {
//              val nextFile = nextFiles.next()
//              if(file.get("accession").asText() == originalSourceId)
//                recursiveItems(nextFile.asText().split("/")(2),finalItemId,description)
//              else
//                recursiveItems(nextFile.asText().split("/")(2),initialItemId,description)
//            }
//          }
//          else {
//            if(file.get("accession").asText() != originalSourceId) {
//              var initialItemId = 0
//              val item = defineItem(file)
//              if (item.checkInsert()) {
//                initialItemId = item.specialInsert()
//              }
//              else {
//                initialItemId = item.specialUpdate()
//              }
//              insertCaseItem(initialItemId)
//              getReplicatesAndInsert(file,initialItemId)
//              val derivedFrom = defineDerivedFrom(file,initialItemId,finalItemId,precDescription)
//              insertOrUpdateDerivedFrom(derivedFrom)
//            }
//          }
//        }
//      }
//    }
//  }
//
//  def getPipelineAndPlatformHelper(sourceId: String): List[String] = {
//    var result: ListBuffer[String] = new ListBuffer[String]()
//    if (rootNode.has("files")) {
//      val files = rootNode.get("files").getElements
//      while (files.hasNext) {
//        val file = files.next()
//        if (file.has("@id") && file.get("@id").asText().contains(sourceId)) {
//          if(file.has("analysis_step_version")){
//            result += this.getPipeline(file)
//          }
//          else
//            result += null
//          if(file.has("platform")) {
//            println(file.findValue("platform").get("term_name").asText())
//            result += file.findValue("platform").get("term_name").asText()
//          }
//          else
//            result += null
//        }
//      }
//    }
//    result.toList
//  }
//
//  def defineItem(file: JsonNode): ItemEncode = {
//    val item = new ItemEncode(encodesTableId)
//    item.experimentTypeId = experimentTypeId
//    item.sourceId = file.get("accession").asText()
//    item.dataType = file.get("output_type").asText()
//    item.format = file.get("file_type").asText()
//    item.size = file.get("file_size").asLong()
//    item.sourceUrl = "https://www.encodeproject.org" + file.get("href").asText()
//    if(file.has("analysis_step_version")){
//      item.pipeline = getPipeline(file)
//    }
//    if(file.has("platform"))
//      item.platform = file.findValue("platform").get("term_name").asText()
//    item
//  }
//
//  def getPipeline(file: JsonNode): String = {
//    var pipelineString: String = null
//    val pipelines = file.findValue("pipelines").getElements
//    while(pipelines.hasNext){
//      val pipeline = pipelines.next()
//      if(pipelineString == null) {
//        pipelineString = pipeline.get("title").asText()
//      }
//      else
//        pipelineString = pipelineString.concat(", " + pipeline.get("title").asText())
//    }
//    pipelineString
//  }
//
//
//  def defineDerivedFrom(file: JsonNode, initialItemId: Int, finalItemId: Int, precDescription: String): DerivedFromEncode = {
//    val derivedFrom = new DerivedFromEncode(encodesTableId)
//    derivedFrom.initialItemId = initialItemId
//    derivedFrom.finalItemId = finalItemId
//    derivedFrom.description = precDescription
//    if(file.has("analysis_step_version")) {
//      setDescription(file)
//    }
//    derivedFrom
//  }
//
//  def setDescription(file: JsonNode): Unit = {
//    val analysis = file.findValue("analysis_step_types").getElements
//    while (analysis.hasNext) {
//      val analys = analysis.next()
//      if(description == null) {
//        description = analys.asText()
//      }
//      else
//        description = description.concat(", " + analys.asText())
//    }
//  }
//
//  def insertOrUpdateDerivedFrom(derivedFrom: DerivedFromEncode): Unit = {
//    if (derivedFrom.checkInsert())
//      derivedFrom.insert()
//    else
//      derivedFrom.update()
//  }
//
//  def insertCaseItem(itemId: Int):Unit = {
//    val caseItem = new CaseItemEncode(encodesTableId)
//    caseItem.itemId = itemId
//    caseItem.caseId = caseId
//    caseItem.insertRow()
//  }
//
//  def getReplicatesAndInsert(file: JsonNode, itemId: Int):Unit ={
//    if(file.has("technical_replicates")){
//      val replicates = file.get("technical_replicates").getElements
//      while(replicates.hasNext) {
//        val replicate = replicates.next()
//        insertReplicateItem(itemId,replicate.asText())
//      }
//    }
//  }
//
//  def insertReplicateItem(itemId: Int, key: String): Unit ={
//    val replicateItem = new ReplicateItemEncode(encodesTableId)
//    replicateItem.itemId = itemId
//    replicateItem.repId = encodesTableId.replicateMap(key)
//    replicateItem.insRow()
//  }
//
//}
