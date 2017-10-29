package it.polimi.genomics.importer.ModelDatabase


class Item extends EncodeTable{

  var containerId : Int = _

  var sourceId : String = _

  var dataType: String = _

  var format : String = _

  var size : Int = _

  var pipeline : String = _

  var sourceUrl : String = _

  var localUrl : String = _

  _hasForeignKeys = true

  _foreignKeysTables = List("CONTAINERS")

  override def setParameter(param: String, dest: String, insertMethod: (String,String) => String): Unit = dest.toUpperCase() match {
    case "SOURCEID" => this.sourceId = insertMethod(this.sourceId,param)
    case "DATATYPE" => this.dataType = insertMethod(this.dataType,param)
    case "FORMAT" => this.format = insertMethod(this.format,param)
    case "SIZE" => this.size = insertMethod(this.size.toString,param).toInt
    case "PIPELINE" => this.pipeline = insertMethod(this.pipeline,param)
    case "SOURCEURL" => this.sourceUrl = insertMethod(this.sourceUrl,param)
    case "LOCALURL" => this.localUrl = insertMethod(this.localUrl,param)
    case _ => noMatching(dest)
  }

  override def insert(): Int = {
    dbHandler.insertItem(containerId,this.sourceId,this.dataType,this.format,this.size,this.pipeline,this.sourceUrl,this.localUrl)
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
}