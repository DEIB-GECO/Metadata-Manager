package it.polimi.genomics.importer.ModelDatabase


class Item extends EncodeTable{

  var replicateId: Int = _

  var containerId : Int = _

  var sourceId : String = _

  var dataType: String = _

  var format : String = _

  var size : Int = _

  var pipeline : String = _

  var sourceUrl : String = _

  var localUrl : String = _

  _hasForeignKeys = true

  _foreignKeysTables = List("REPLICATES","CONTAINERS")

  override def setParameter(param: String, dest: String): Unit = dest.toUpperCase() match {
    case "SOURCEID" => this.sourceId = setValue(this.sourceId,param)
    case "DATATYPE" => this.dataType = setValue(this.dataType,param)
    case "FORMAT" => this.format = setValue(this.format,param)
    case "SIZE" => this.size = param.toInt
    case "PIPELINE" => this.pipeline = setValue(this.pipeline,param)
    case "SOURCEURL" => this.sourceUrl = setValue(this.sourceUrl,param)
    case "LOCALURL" => this.localUrl = setValue(this.localUrl,param)
    case _ => noMatching(dest)
  }

  override def insert() = {
    dbHandler.insertItem(replicateId,containerId,this.sourceId,this.dataType,this.format,this.size,this.pipeline,this.sourceUrl,this.localUrl)
  }

  override def setForeignKeys(table: Table): Unit = {
    if(table.isInstanceOf[Replicate])
      this.replicateId = table.getId
    if(table.isInstanceOf[Container])
      this.containerId = table.getId
  }

  override def checkInsert(): Boolean ={
    dbHandler.checkInsertItem(this.sourceId)
  }

  override def getId() = {
    dbHandler.getItemId(this.sourceId)
  }
}