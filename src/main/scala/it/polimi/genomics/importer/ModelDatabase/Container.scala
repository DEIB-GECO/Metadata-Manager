package it.polimi.genomics.importer.ModelDatabase

import java.io.{File, FileOutputStream, PrintWriter}

import it.polimi.genomics.importer.ModelDatabase.Utils.Statistics

trait Container extends Table{

  var experimentTypeId : Int = _

  var name : String = _

  var assembly: String = _

  var isAnn: Boolean = _

  var annotation: String = null

  _hasForeignKeys = true

  _foreignKeysTables = List("EXPERIMENTSTYPE")

  _hasDependencies = true

  _dependenciesTables = List("CONTAINERS", "DONOR")

  override def insert() : Int ={
    dbHandler.insertContainer(experimentTypeId,this.name,this.assembly,this.isAnn,this.annotation)
  }

  override def update() : Int ={
    dbHandler.updateContainer(experimentTypeId,this.name,this.assembly,this.isAnn,this.annotation)
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

  override def checkConsistency(): Boolean = {
    if(this.name != null) true else false
  }

  override def checkDependenciesSatisfaction(table: Table): Boolean = {
    table match {
      case container: Container => {
        if (container.isAnn && container.annotation == null){
          Statistics.constraintsViolated += 1
          this.logger.warn("Container annotation constrains violated")
          return false
        }
        true
      }
      case donor: Donor =>{
        if(donor.species.toUpperCase().equals("HOMO SAPIENS") && !(this.assembly.equals("hg19") || this.assembly.equals("GRh38"))) {
          Statistics.constraintsViolated += 1
          this.logger.warn("Container species constrains violated")
          return false
        }
        true
      }
      case _ => true
    }
  }

  def convertTo(values: Seq[(Int, Int, String, Option[String], Boolean, Option[String])]): Unit = {
    if(values.length > 1)
      logger.error(s"Too many value: ${values.length}")
    else {
      var value = values.head
      this.primaryKey_(value._1)
      this.experimentTypeId = value._2
      this.name = value._3
      if(value._4.isDefined) this.assembly = value._4.get
      this.isAnn = value._5
      if(value._6.isDefined) this.annotation = value._6.get
    }
  }

  def writeInFile(path: String): Unit = {
    val write = getWriter(path)
    val tableName = "container"
    write.append(getMessage(tableName + "__name", this.name))
    if(this.assembly != null) write.append(getMessage(tableName + "__types", this.assembly))
    write.append(getMessage(tableName + "__is_ann", this.isAnn))
    if(this.annotation != null) write.append(getMessage(tableName + "__annotation", this.annotation))
    flushAndClose(write)
  }
}
