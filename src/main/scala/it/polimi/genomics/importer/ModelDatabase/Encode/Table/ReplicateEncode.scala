package it.polimi.genomics.importer.ModelDatabase.Encode.Table

import it.polimi.genomics.importer.ModelDatabase.Encode.EncodeTableId
import it.polimi.genomics.importer.ModelDatabase.{Replicate, Table}

import scala.collection.mutable.ListBuffer


class ReplicateEncode(encodeTableId: EncodeTableId) extends EncodeTable(encodeTableId) with Replicate {

  var bioSampleIdList : ListBuffer[Int] = new ListBuffer[Int]

  var sourceIdList : ListBuffer[String] = new ListBuffer[String]

  //var platform : ListBuffer[String] = new ListBuffer[String]

  var bioReplicateNumList : ListBuffer[Int] = new ListBuffer[Int]

  var techReplicateNumList : ListBuffer[Int] = new ListBuffer[Int]

  _hasForeignKeys = true

  _foreignKeysTables = List("BIOSAMPLES")

  var actualPosition : Int = _

  override def setParameter(param: String, dest: String, insertMethod: (String,String) => String): Unit = {
    dest.toUpperCase match{
      case "SOURCEID" => {this.sourceIdList += insertMethod(param,param)}
      case "BIOREPLICATENUM" => {this.bioReplicateNumList += param.toInt}
      case "TECHREPLICATENUM" => {this.techReplicateNumList += param.toInt}
      case _ => noMatching(dest)
    }
  }

  override def insertRow(): Unit ={
    var id: Int = 0
    this.sourceIdList.map(source=>{
      println(source)
      this.actualPosition = sourceIdList.indexOf(source)
      if(this.checkInsert()) {
        id = this.insert
      }
      else {
        id = this.update
      }
      this.primaryKeys_(id)
      val replicateKey: String = bioReplicateNumList(this.actualPosition).toString() + "_" + techReplicateNumList(this.actualPosition).toString()
      encodeTableId.replicateMap_(replicateKey,id)
    })
  }

  override def insert(): Int = {
    this.dbHandler.insertReplicate(this.bioSampleIdList(this.actualPosition),this.sourceIdList(this.actualPosition),this.bioReplicateNumList(this.actualPosition),this.techReplicateNumList(this.actualPosition))
  }

  override def update(): Int = {
    this.dbHandler.updateReplicate(this.bioSampleIdList(this.actualPosition),this.sourceIdList(this.actualPosition),this.bioReplicateNumList(this.actualPosition),this.techReplicateNumList(this.actualPosition))
  }

  override def setForeignKeys(table: Table): Unit = {
    println("Set Foreign Keys")
    this.bioSampleIdList = table.primaryKeys
    println(this.bioSampleId)
  }

  override def checkInsert(): Boolean ={
     dbHandler.checkInsertReplicate(this.sourceIdList(actualPosition))
  }

  override def getId(): Int = {
    dbHandler.getReplicateId(this.sourceIdList(actualPosition))
  }

  override def checkConsistency(): Boolean = {
    //if(this.sourceId != null) true else false
    this.sourceIdList.foreach(source => if(source == null) false)
    true
  }
}
