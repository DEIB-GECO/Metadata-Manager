package it.polimi.genomics.importer.ModelDatabase

import it.polimi.genomics.importer.ModelDatabase.Utils.Statistics

trait Item extends Table{

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

  override def insert(): Int = {
    val id = dbHandler.insertItem(containerId,this.sourceId,this.dataType,this.format,this.size,this.platform,this.pipeline,this.sourceUrl,this.localUrl)
    Statistics.itemInserted += 1
    id
  }

  override def update(): Int = {
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
