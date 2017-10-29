package it.polimi.genomics.importer.ModelDatabase


class Container extends EncodeTable{

  var experimentTypeId : Int = _

  var name : String = "ENCODE"

  var assembly: String = _

  var isAnn: Boolean = _

  var annotation: String = null

  _hasForeignKeys = true

  _foreignKeysTables = List("EXPERIMENTSTYPE")

  override def setParameter(param: String, dest: String,insertMethod: (String,String) => String): Unit = dest.toUpperCase match {
    case "NAME" => this.name = insertMethod(this.name,param)
    case "ASSEMBLY" => this.assembly = insertMethod(this.assembly,param)
   /* case "ISANN" => this.isAnn = false   //manually curated
    case "ANNOTATION" => this.annotation = null     //manually curated*/
    case "ISANN" => this.isAnn = if(insertMethod(this.isAnn.toString,param).equals("true")) true else false
    case "ANNOTATION" => this.annotation = insertMethod(this.annotation,param)
    case _ => noMatching(dest)
  }

  override def insert() : Int ={
    dbHandler.insertContainer(experimentTypeId,this.name,this.assembly,this.isAnn,this.annotation)
  }

  override def setForeignKeys(table: Table): Unit = {
    this.experimentTypeId = table.primaryKey
  }

  override def checkInsert(): Boolean ={
    dbHandler.checkInsertContainer(this.name)
  }

  override def getId(): Int = {
    dbHandler.getContainerId(this.name)
  }
}
