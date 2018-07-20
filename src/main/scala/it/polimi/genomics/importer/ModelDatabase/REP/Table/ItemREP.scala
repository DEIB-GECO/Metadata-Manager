package it.polimi.genomics.importer.ModelDatabase.REP.Table

import it.polimi.genomics.importer.ModelDatabase.Encode.EncodeTableId
import it.polimi.genomics.importer.ModelDatabase.REP.REPTableId
//import it.polimi.genomics.importer.ModelDatabase.Encode.Utils.PlatformRetriver
import it.polimi.genomics.importer.ModelDatabase.Item
import it.polimi.genomics.importer.ModelDatabase.Utils.Statistics


class ItemREP(repTableId: REPTableId) extends REPTable(repTableId) with Item {

  override def setParameter(param: String, dest: String, insertMethod: (String,String) => String): Unit = dest.toUpperCase() match {
    case "SOURCEID" => this.sourceId = insertMethod(this.sourceId,param)
    case "SIZE" => this.size = insertMethod(this.size.toString,param).toLong
    case "DATE" => this.date = insertMethod(this.size.toString,param)
    case "CHECKSUM" => this.checksum = insertMethod(this.size.toString,param)
    case "PLATFORM" => this.platform = insertMethod(this.platform, param)
    case "PIPELINE" => this.pipeline = insertMethod(this.pipeline,param)
    case "SOURCEURL" => this.sourceUrl = insertMethod(this.sourceUrl,param)
    case "LOCALURL" => this.localUrl = insertMethod(this.localUrl,param)
    case _ => noMatching(dest)
  }

  override def insert(): Int = {
    val id = dbHandler.insertItem(experimentTypeId, datasetId,this.sourceId, this.size, this.date, this.checksum, this.platform, this.pipeline, this.sourceUrl, this.localUrl)
    Statistics.itemInserted += 1
    id
  }

  override def update(): Int = {
    val id = dbHandler.updateItem(experimentTypeId,datasetId,this.sourceId,this.size, this.date,this.checksum,this.platform,this.pipeline,this.sourceUrl, this.localUrl)
    Statistics.itemUpdated += 1
    id
  }

  override def updateById(): Unit = {
    val id = dbHandler.updateItemById(this.primaryKey, experimentTypeId,datasetId,this.sourceId,this.size,this.date,this.checksum,this.platform,this.pipeline,this.sourceUrl, this.localUrl)
    Statistics.itemUpdated += 1
  }




}