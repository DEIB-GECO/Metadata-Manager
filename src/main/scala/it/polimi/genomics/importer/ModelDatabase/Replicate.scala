package it.polimi.genomics.importer.ModelDatabase

import scala.collection.mutable.ListBuffer


class Replicate extends EncodeTable{

  var bioSampleId : Int = _

  var sourceId : ListBuffer[String] = new ListBuffer[String]

  var bioReplicateNum : ListBuffer[Int] = new ListBuffer[Int]

  var techReplicateNum : ListBuffer[Int] = new ListBuffer[Int]

  _hasForeignKeys = true

  _foreignKeysTables = List("BIOSAMPLES")

  override def setParameter(param: String, dest: String, insertMethod: (String,String) => String): Unit = {
    dest.toUpperCase match{
      case "SOURCEID" =>{ this.sourceId += param}
      case "BIOREPLICATENUM" => this.bioReplicateNum += param.toInt
      case "TECHREPLICATENUM" => this.techReplicateNum += param.toInt
      case _ => noMatching(dest)
    }
  }

  override def insertRow(): Unit ={
    this.sourceId.map(source=>{
      if(this.checkInsertReplicate(source)) {
        val id = this.insertReplicate(source)
        this.primaryKey_(id)
    }
    else {
      val id = this.getIdReplicate(source)
      this.primaryKey_(id)
    }})
  }

  override def insert(): Int = {
     this.dbHandler.insertReplicate(this.bioSampleId,this.sourceId.head,this.bioReplicateNum.head,this.techReplicateNum.head)
  }

   def insertReplicate(sourceId: String): Int = {
    this.dbHandler.insertReplicate(this.bioSampleId,sourceId,this.bioReplicateNum.head,this.techReplicateNum.head)
  }

  override def setForeignKeys(table: Table): Unit = {
    this.bioSampleId = table.primaryKey
  }

  override def checkInsert(): Boolean ={
     dbHandler.checkInsertReplicate(this.sourceId.head)
  }

  def checkInsertReplicate(sourceId: String) = {
    dbHandler.checkInsertReplicate(sourceId)
  }

  override def primaryKey_(key: Int): Unit = {
    _primaryKeys += key
  }

  override def primaryKey(): Int ={
    _primaryKeys.head
  }

  override def getId(): Int = {
    dbHandler.getReplicateId(this.sourceId.head)
  }

  def getIdReplicate(sourceId: String): Int = {
    dbHandler.getReplicateId(sourceId)
  }
}
