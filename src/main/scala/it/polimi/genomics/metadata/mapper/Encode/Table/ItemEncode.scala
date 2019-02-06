package it.polimi.genomics.metadata.mapper.Encode.Table

import it.polimi.genomics.metadata.mapper.Encode.EncodeTableId
import it.polimi.genomics.metadata.mapper.Pair
//import it.polimi.genomics.importer.ModelDatabase.Encode.Utils.PlatformRetriver
import it.polimi.genomics.metadata.mapper.Utils.Statistics
import it.polimi.genomics.metadata.mapper.Item


class ItemEncode(encodeTableId: EncodeTableId) extends EncodeTable(encodeTableId) with Item {

  override def setParameter(param: String, dest: String, insertMethod: (String,String) => String): Unit = dest.toUpperCase() match {
    case "SOURCEID" => this.sourceId = insertMethod(this.sourceId,param)
    case "SIZE" => this.size = insertMethod(this.size.toString,param).toLong
    case "DATE" => this.date = insertMethod(this.date,param)
    case "CHECKSUM" => this.checksum = insertMethod(this.checksum,param)
    case "CONTENTTYPE" => this.contentType = insertMethod(this.contentType,param)
    case "PLATFORM" => this.platform = sortPipelineOrPlatform(insertMethod(this.platform, param))
    case "PIPELINE" => this.pipeline = sortPipelineOrPlatform(insertMethod(this.pipeline,param))
    case "SOURCEURL" => this.sourceUrl = insertMethod(this.sourceUrl,param)
    case "LOCALURL" => this.localUrl = insertMethod(this.localUrl,param)
    case "FILENAME" => this.fileName = insertMethod(this.fileName,param)
    case _ => noMatching(dest)
  }

  def sortPipelineOrPlatform(s: String): String = {
    val pArray: Array[String] = s.split(",")
    val pArrayNoSpaces = pArray.map(x => if(x.startsWith(" ")) x.drop(1) else x)
    val pArraySorted = pArrayNoSpaces.sorted
    pArraySorted.mkString(", ")
  }

  override def insert(): Int = {
    val id = dbHandler.insertItem(experimentTypeId, datasetId,this.sourceId, this.size, this.date, this.checksum, this.contentType, this.platform, this.pipeline, this.sourceUrl, this.localUrl, this.fileName)
    Statistics.itemInserted += 1
    id
  }

  override def update(): Int = {
    val id = dbHandler.updateItem(experimentTypeId,datasetId,this.sourceId,this.size,this.date,this.checksum,this.contentType,this.platform,this.pipeline,this.sourceUrl, this.localUrl, this.fileName)
    Statistics.itemUpdated += 1
    id
  }

  override def updateById(): Unit = {
    val id = dbHandler.updateItemById(this.primaryKey, experimentTypeId,datasetId,this.sourceId,this.size,this.date,this.checksum,this.contentType,this.platform,this.pipeline,this.sourceUrl, this.localUrl, this.fileName)
    Statistics.itemUpdated += 1
  }

}