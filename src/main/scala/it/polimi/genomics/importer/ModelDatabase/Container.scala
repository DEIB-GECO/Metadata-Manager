package it.polimi.genomics.importer.ModelDatabase

import it.polimi.genomics.importer.ModelDatabase.Utils.Statistics

trait Container extends Table{

  var projectId : Int = _

  var name : String = _

  var assembly: String = _

  var isAnn: Boolean = _

  var annotation: String = _

  _hasForeignKeys = false

//  _foreignKeysTables = List("PROJECTS")

  _hasDependencies = true

  _dependenciesTables = List("CONTAINERS", "DONORS")

  override def insert() : Int ={
    dbHandler.insertContainer(this.name,this.assembly,this.isAnn,this.annotation)
  }

  override def update() : Int ={
    dbHandler.updateContainer(this.name,this.assembly,this.isAnn,this.annotation)
  }

  override def updateById() : Unit ={
    dbHandler.updateContainerById(primaryKey, this.name,this.assembly,this.isAnn,this.annotation)
  }

  override def setForeignKeys(table: Table): Unit = {
    this.projectId = table.primaryKey
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
    try {
      table match {
        case container: Container => {
          if (container.isAnn && container.annotation == null) {
            Statistics.constraintsViolated += 1
            this.logger.warn("Container annotation constrains violated")
            return false
          }
          true
        }
        case donor: Donor => {
          if (donor.species.toUpperCase().equals("HOMO SAPIENS") && !(this.assembly.equals("hg19") || this.assembly.equals("GRCh38"))) {
            Statistics.constraintsViolated += 1
            this.logger.warn("Container species constrains violated")
            return false
          }
          true
        }
        case _ => true
      }
    } catch {
      case e: Exception => {
        logger.warn("java.lang.NullPointerException")
        true
      };
    }
  }

  def convertTo(values: Seq[(Int, String, Option[String], Option[Boolean], Option[String])]): Unit = {
    if(values.length > 1)
      logger.error(s"Too many value: ${values.length}")
    else {
      var value = values.head
      this.primaryKey_(value._1)
      this.name = value._2
      if(value._3.isDefined) this.assembly = value._3.get
      if(value._4.isDefined) this.isAnn = value._4.get
      if(value._5.isDefined) this.annotation = value._5.get
    }
  }

  def writeInFile(path: String): Unit = {
    val write = getWriter(path)
    val tableName = "container"
    write.append(getMessage(tableName, "name", this.name))
    if(this.assembly != null) write.append(getMessage(tableName, "assembly", this.assembly))
    write.append(getMessage(tableName, "is_ann", this.isAnn))
    if(this.annotation != null) write.append(getMessage(tableName, "annotation", this.annotation))
    flushAndClose(write)
  }
}
