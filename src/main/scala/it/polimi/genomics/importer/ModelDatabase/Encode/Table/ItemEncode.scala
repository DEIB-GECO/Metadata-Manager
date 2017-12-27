package it.polimi.genomics.importer.ModelDatabase.Encode.Table

import it.polimi.genomics.importer.ModelDatabase.Encode.EncodeTableId
import it.polimi.genomics.importer.ModelDatabase.Encode.Utils.PlatformRetriver
import it.polimi.genomics.importer.ModelDatabase.Utils.Statistics
import it.polimi.genomics.importer.ModelDatabase.Item


class ItemEncode(encodeTableId: EncodeTableId) extends EncodeTable(encodeTableId) with Item {

  override def setParameter(param: String, dest: String, insertMethod: (String,String) => String): Unit = dest.toUpperCase() match {
    case "SOURCEID" => this.sourceId = insertMethod(this.sourceId,param)
    case "DATATYPE" => this.dataType = insertMethod(this.dataType,param)
    case "FORMAT" => this.format = insertMethod(this.format,param)
    case "SIZE" => this.size = insertMethod(this.size.toString,param).toLong
    case "PLATFORM" => this.platform = insertMethod(this.platform, param)
    case "PIPELINE" => this.pipeline = insertMethod(this.pipeline,param)
    case "SOURCEURL" => this.sourceUrl = insertMethod(this.sourceUrl,param)
    case "LOCALURL" => this.localUrl = insertMethod(this.localUrl,param)
    case _ => noMatching(dest)
  }

  override def insert(): Int = {
    val id = dbHandler.insertItem(containerId,this.sourceId,this.dataType,this.format,this.size,this.platform,this.pipeline,this.sourceUrl,this.localUrl)
    val platformRetriver = new PlatformRetriver(this.filePath, this.sourceId,this.encodeTableId)
    platformRetriver.getItems(id,this.containerId,this.encodeTableId.caseId)
    Statistics.itemInserted += 1
    id
  }

  def specialInsert(): Int ={
    val id = dbHandler.insertItem(containerId,this.sourceId,this.dataType,this.format,this.size,this.platform,this.pipeline,this.sourceUrl,this.localUrl)
    Statistics.itemInserted += 1
    id
  }


  override def update(): Int = {
    val id = dbHandler.updateItem(containerId,this.sourceId,this.dataType,this.format,this.size,this.platform,this.pipeline,this.sourceUrl,this.localUrl)
    val platformRetriver = new PlatformRetriver(this.filePath, this.sourceId,this.encodeTableId)
    platformRetriver.getItems(id,this.containerId,this.encodeTableId.caseId)
    Statistics.itemInserted += 1
    id
  }

  def specialUpdate(): Int ={
    val id = dbHandler.updateItem(containerId,this.sourceId,this.dataType,this.format,this.size,this.platform,this.pipeline,this.sourceUrl,this.localUrl)
    Statistics.itemInserted += 1
    id
  }
}