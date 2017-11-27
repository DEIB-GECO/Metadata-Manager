package it.polimi.genomics.importer.ModelDatabase

import it.polimi.genomics.importer.ModelDatabase.Utils.PlatformRetriver

import scala.collection.mutable.ListBuffer


class Replicate extends EncodeTable{

  var bioSampleId : Int = _

  var sourceId : ListBuffer[String] = new ListBuffer[String]

  //var platform : ListBuffer[String] = new ListBuffer[String]

  var bioReplicateNum : ListBuffer[Int] = new ListBuffer[Int]

  var techReplicateNum : ListBuffer[Int] = new ListBuffer[Int]

  _hasForeignKeys = true

  _foreignKeysTables = List("BIOSAMPLES")

  var actualPosition : Int = _

  override def setParameter(param: String, dest: String, insertMethod: (String,String) => String): Unit = {
    dest.toUpperCase match{
      case "SOURCEID" =>{ this.sourceId += insertMethod(param,param);
                          //this.platform += new PlatformRetriver(this.filePath).getPlatform(param)
      }
      //case "PLATFORM" =>{ this.platform += insertMethod("","")}
      case "BIOREPLICATENUM" => this.bioReplicateNum += param.toInt
      case "TECHREPLICATENUM" => this.techReplicateNum += param.toInt
      case _ => noMatching(dest)
    }
  }

  override def insertRow(): Unit ={
    this.sourceId.map(source=>{
      this.actualPosition = sourceId.indexOf(source)
      if(this.checkInsertReplicate(source)) {

        //val id = this.insertReplicate(sourceId.indexOf(source))
        val id = this.insert
        this.primaryKeys_(id)
    }
    else {
      //val id = this.getIdReplicate(source)
      val id = this.update
      this.primaryKeys_(id)
    }})
  }

  override def insert(): Int = {
    this.dbHandler.insertReplicate(this.bioSampleId,this.sourceId(this.actualPosition),this.bioReplicateNum(this.actualPosition),this.techReplicateNum(this.actualPosition))

  }

  override def update(): Int = {
    this.dbHandler.updateReplicate(this.bioSampleId,this.sourceId(this.actualPosition),this.bioReplicateNum(this.actualPosition),this.techReplicateNum(this.actualPosition))
  }

  /* def insertReplicate(position: Int): Int = {
    this.dbHandler.insertReplicate(this.bioSampleId,this.sourceId(position),this.bioReplicateNum(position),this.techReplicateNum(position))
  }

  def updateReplicate(position: Int): Int = {
    this.dbHandler.updateReplicate(this.bioSampleId,this.sourceId(position),this.bioReplicateNum(position),this.techReplicateNum(position))
  }*/

  override def setForeignKeys(table: Table): Unit = {
    this.bioSampleId = table.primaryKey
  }

  override def checkInsert(): Boolean ={
     dbHandler.checkInsertReplicate(this.sourceId.head)
  }

  def checkInsertReplicate(sourceId: String) = {
    dbHandler.checkInsertReplicate(sourceId)
  }

 /* override def primaryKey_(key: Int): Unit = {
    _primaryKeys += key
  }

  override def primaryKey(): Int ={
    _primaryKeys.head
  }*/

  override def getId(): Int = {
    dbHandler.getReplicateId(this.sourceId.head)
  }

  def getIdReplicate(sourceId: String): Int = {
    dbHandler.getReplicateId(sourceId)
  }

  override def checkConsistency(): Boolean = {
    if(this.sourceId != null) true else false
  }
}
