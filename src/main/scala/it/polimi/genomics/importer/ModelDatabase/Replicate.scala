package it.polimi.genomics.importer.ModelDatabase

import it.polimi.genomics.importer.ModelDatabase.Utils.PlatformRetriver

import scala.collection.mutable.ListBuffer


class Replicate(encodeTableId: EncodeTableId) extends EncodeTable(encodeTableId){

  var bioSampleId : ListBuffer[Int] = new ListBuffer[Int]

  var sourceId : ListBuffer[String] = new ListBuffer[String]

  //var platform : ListBuffer[String] = new ListBuffer[String]

  var bioReplicateNum : ListBuffer[Int] = new ListBuffer[Int]

  var techReplicateNum : ListBuffer[Int] = new ListBuffer[Int]

  _hasForeignKeys = true

  _foreignKeysTables = List("BIOSAMPLES")

  var actualPosition : Int = _

  override def setParameter(param: String, dest: String, insertMethod: (String,String) => String): Unit = {
    dest.toUpperCase match{
      case "SOURCEID" => {this.sourceId += insertMethod(param,param); println("sourceid " + param)}
      case "BIOREPLICATENUM" => {this.bioReplicateNum += param.toInt; println(" biorepnumb" + param)}
      case "TECHREPLICATENUM" => {this.techReplicateNum += param.toInt; println("technumber " + param)}
      case _ => noMatching(dest)
    }
  }

  override def insertRow(): Unit ={
    var id: Int = 0
    this.sourceId.map(source=>{
      println(source)
      this.actualPosition = sourceId.indexOf(source)
      if(this.checkInsert()) {
        id = this.insert
      }
      else {
        id = this.update
      }
      this.primaryKeys_(id)
      val replicateKey = bioReplicateNum(this.actualPosition).toString() + "_" + techReplicateNum(this.actualPosition).toString()
      encodeTableId.replicateMap_(replicateKey,id)
    })
  }

  override def insert(): Int = {
    this.dbHandler.insertReplicate(this.bioSampleId(this.actualPosition),this.sourceId(this.actualPosition),this.bioReplicateNum(this.actualPosition),this.techReplicateNum(this.actualPosition))
  }

  override def update(): Int = {
    this.dbHandler.updateReplicate(this.bioSampleId(this.actualPosition),this.sourceId(this.actualPosition),this.bioReplicateNum(this.actualPosition),this.techReplicateNum(this.actualPosition))
  }

  override def setForeignKeys(table: Table): Unit = {
    println("Set Foreign Keys")
    this.bioSampleId = table.primaryKeys
    println(this.bioSampleId)
  }

  override def checkInsert(): Boolean ={
     dbHandler.checkInsertReplicate(this.sourceId(actualPosition))
  }

  override def getId(): Int = {
    dbHandler.getReplicateId(this.sourceId.head)
  }

  def getIdReplicate(sourceId: String): Int = {
    dbHandler.getReplicateId(sourceId)
  }

  override def checkConsistency(): Boolean = {
    //if(this.sourceId != null) true else false
    this.sourceId.foreach(source => if(this.sourceId == null) false)
    true
  }
}
