package it.polimi.genomics.metadata.mapper

import it.polimi.genomics.metadata.mapper.Utils.Statistics

trait Dataset extends Table{

  var name : String = _

  var dataType : String = _

  var format : String = _

  var assembly: String = _

  var isAnn: Boolean = _

  _hasForeignKeys = false

//  _foreignKeysTables = List("PROJECTS")

  _hasDependencies = true

  _dependenciesTables = List("DATASETS", "DONORS")

  override def insert() : Int ={
    dbHandler.insertDataset(this.name,this.dataType,this.format,this.assembly,this.isAnn)
  }

  override def update() : Int ={
    dbHandler.updateDataset(this.name,this.dataType,this.format,this.assembly,this.isAnn)
  }

  override def updateById() : Unit ={
    dbHandler.updateDatasetById(primaryKey,this.name,this.dataType,this.format,this.assembly,this.isAnn)
  }

  /*override def setForeignKeys(table: Table): Unit = {
    this.projectId = table.primaryKey
  }*/

  override def checkInsert(): Boolean ={
    dbHandler.checkInsertDataset(this.name)
  }

  override def getId(): Int = {
    dbHandler.getDatasetId(this.name)
  }

  override def checkConsistency(): Boolean = {
    if(this.dataType != null || this.format!=null || this.assembly!=null) true else false
  }

  override def checkDependenciesSatisfaction(table: Table): Boolean = {
    try {
      table match {
        case dataset: Dataset => {
          /*if (dataset.isAnn) {
            Statistics.constraintsViolated += 1
            this.logger.warn("Dataset annotation constraints violated")
            return false
          }*/
          true
        }
        case donor: Donor => {
          if (donor.species.toUpperCase().equals("HOMO SAPIENS") && !(this.assembly.equals("hg19") || this.assembly.equals("GRCh38"))) {
            Statistics.constraintsViolated += 1
            this.logger.warn("Dataset species constraints violated")
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

  def convertTo(values: Seq[(Int, String, Option[String], Option[String], Option[String], Option[Boolean])]): Unit = {
    if(values.length > 1)
      logger.error(s"Too many values: ${values.length}")
    else {
      var value = values.head
      this.primaryKey_(value._1)
      this.name = value._2
      if(value._3.isDefined) this.dataType = value._3.get
      if(value._4.isDefined) this.format = value._4.get
      if(value._5.isDefined) this.assembly = value._5.get
      if(value._6.isDefined) this.isAnn = value._6.get
    }
  }

  def writeInFile(path: String): Unit = {
    val write = getWriter(path)
    val tableName = "dataset"
    write.append(getMessage(tableName, "name", this.name))
    if(this.dataType != null) write.append(getMessage(tableName, "data_type", this.dataType))
    if(this.format != null) write.append(getMessage(tableName, "format", this.format))
    if(this.assembly != null) write.append(getMessage(tableName, "assembly", this.assembly))
    write.append(getMessage(tableName, "is_ann", this.isAnn))
    flushAndClose(write)
  }
}
