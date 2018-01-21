package it.polimi.genomics.importer.ModelDatabase

import java.io.{File, FileOutputStream, PrintWriter}

trait ExperimentType extends Table{

  var technique : String = _

  var feature : String = _

  var target : String = _

  var antibody : String = _

  override def checkInsert(): Boolean = {
    dbHandler.checkInsertExperimentType(this.technique)
  }

  override def insert(): Int = {
    dbHandler.insertExperimentType(this.technique,this.feature,this.target,this.antibody)
  }

  override def update(): Int = {
    dbHandler.updateExperimentType(this.technique,this.feature,this.target,this.antibody)
  }

  override def checkConsistency(): Boolean = {
    if(this.technique != null) true else false
  }

  override def setForeignKeys(table: Table): Unit = {

  }

  override def getId(): Int = {
    dbHandler.getExperimentTypeId(this.technique)
  }

  def convertTo(values: Seq[(Int, String, Option[String], Option[String], Option[String])]): Unit = {
    if(values.length > 1)
      logger.error(s"Too many value: ${values.length}")
    else {
      var value = values.head
      this.primaryKey_(value._1)
      this.technique = value._2
      if(value._3.isDefined) this.feature = value._3.get
      if(value._4.isDefined) this.target = value._4.get
      if(value._5.isDefined) this.antibody = value._5.get
    }
  }

  def writeInFile(path: String): Unit = {
    val write = getWriter(path)
    val tableName = "experimenttype"

    write.append(getMessage(tableName + "_technique", this.technique))
    if(this.feature != null) write.append(getMessage(tableName + "_feature", this.feature))
    if(this.target != null) write.append(getMessage(tableName + "_age", this.target))
    if(this.antibody != null) write.append(getMessage(tableName + "_antibody", this.antibody))
    flushAndClose(write)
  }

}
