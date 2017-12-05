package it.polimi.genomics.importer.ModelDatabase

import it.polimi.genomics.importer.ModelDatabase.Utils.{PlatformRetriver, Statistics}


class Item extends EncodeTable{

  var containerId : Int = _

  var sourceId : String = _

  var dataType: String = _

  var format : String = _

  var size : Long = _

  var pipeline : String = _

  var platform : String =_

  var sourceUrl : String = _

  var localUrl : String = _

  _hasForeignKeys = true

  _foreignKeysTables = List("CONTAINERS")

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

  override def insertRow(): Unit ={
    if(this.checkInsert()) {
      val id = this.insert
      this.primaryKey_(id)
    }
    else {
      //val id = this.getId
      val id = this.update

      this.primaryKey_(id)
    }
  }

  override def insert(): Int = {
    val id = dbHandler.insertItem(containerId,this.sourceId,this.dataType,this.format,this.size,this.platform,this.pipeline,this.sourceUrl,this.localUrl)
    val platformRetriver = new PlatformRetriver(this.filePath, this.sourceId)
    platformRetriver.getItems(id,this.containerId,EncodesTableId.caseId)
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
    val platformRetriver = new PlatformRetriver(this.filePath, this.sourceId)
    platformRetriver.getItems(id,this.containerId,EncodesTableId.caseId)
    Statistics.itemInserted += 1
    id
  }

  def specialUpdate(): Int ={
    val id = dbHandler.updateItem(containerId,this.sourceId,this.dataType,this.format,this.size,this.platform,this.pipeline,this.sourceUrl,this.localUrl)
    Statistics.itemInserted += 1
    id
  }

  override def setForeignKeys(table: Table): Unit = {
      this.containerId = table.primaryKey
  }

  override def checkInsert(): Boolean ={
    dbHandler.checkInsertItem(this.sourceId)
  }

  override def getId(): Int = {
    dbHandler.getItemId(this.sourceId)
  }

  override def checkConsistency(): Boolean = {
    if(this.sourceId != null) true else false
  }
}