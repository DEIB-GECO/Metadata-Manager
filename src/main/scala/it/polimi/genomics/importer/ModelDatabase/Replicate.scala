package it.polimi.genomics.importer.ModelDatabase

import java.io.{File, FileOutputStream, PrintWriter}

trait Replicate extends Table{

  var bioSampleId : Int = _

  var sourceId : String = _

  var bioReplicateNum : Int = _

  var techReplicateNum : Int = _

  _hasForeignKeys = true

  _foreignKeysTables = List("BIOSAMPLES")


  override def insert(): Int = {
    this.dbHandler.insertReplicate(this.bioSampleId,this.sourceId,this.bioReplicateNum,this.techReplicateNum)
  }

  override def update(): Int = {
    this.dbHandler.updateReplicate(this.bioSampleId,this.sourceId,this.bioReplicateNum,this.techReplicateNum)
  }

  override def updateById(): Unit = {
    this.dbHandler.updateReplicateById(this.primaryKey, this.bioSampleId,this.sourceId,this.bioReplicateNum,this.techReplicateNum)
  }

  override def setForeignKeys(table: Table): Unit = {
    this.bioSampleId = table.primaryKey
  }

  override def checkInsert(): Boolean ={
     dbHandler.checkInsertReplicate(this.sourceId)
  }

  override def getId(): Int = {
    dbHandler.getReplicateId(this.sourceId)
  }

  override def checkConsistency(): Boolean = {
    if(this.sourceId != null) true else false
  }

  def convertTo(values: Seq[(Int, String, Option[Int], Option[Int])]): Unit = {
    if(values.length > 1)
      logger.error(s"Too many value: ${values.length}")
    else {
      var value = values.head
      this.bioSampleId = value._1
      this.sourceId = value._2
      if(value._3.isDefined) this.bioReplicateNum = value._3.get
      if(value._4.isDefined) this.techReplicateNum = value._4.get
    }
  }

  def writeInFile(path: String): Unit = {
    val write = getWriter(path)
    val tableName = "replicate"
    write.append(getMessage(tableName + "__source_id", this.sourceId))
    if(this.bioReplicateNum != 0) write.append(getMessage(tableName + "__bioReplicate_num", this.bioReplicateNum))
    if(this.techReplicateNum != 0) write.append(getMessage(tableName + "_techReplicate_num", this.techReplicateNum))

    flushAndClose(write)
  }

  def getReplicateIdList(): List[Int] = { List (bioSampleId) }

}
