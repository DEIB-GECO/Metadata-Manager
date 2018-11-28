package it.polimi.genomics.metadata.mapper

import it.polimi.genomics.metadata.mapper.Utils.Statistics

trait BioSample extends Table{

  var donorId: Int = _

  var sourceId: String = _

  var types: String = _

  var tissue: String = _

  var cellLine: String = _

  var isHealthy: Option[Boolean] = _

  var disease: Option[String] = _

  _hasForeignKeys = true

  _foreignKeysTables = List("DONORS")

  _hasDependencies = true

  _dependenciesTables = List("BIOSAMPLES")

  override def insert(): Int ={
    dbHandler.insertBioSample(donorId,this.sourceId,this.types,this.tissue,this.cellLine, this.isHealthy,this.disease)
  }

  override def update(): Int = {
    dbHandler.updateBioSample(donorId,this.sourceId,this.types,this.tissue,this.cellLine, this.isHealthy,this.disease)
  }

  override def updateById(): Unit = {
    dbHandler.updateBioSampleById(this.primaryKey, donorId,this.sourceId,this.types,this.tissue,this.cellLine,this.isHealthy,this.disease)
  }

  override def setForeignKeys(table: Table): Unit = {
    this.donorId = table.primaryKey
  }

  override def checkInsert(): Boolean ={
    dbHandler.checkInsertBioSample(this.sourceId)
  }

  override def getId(): Int = {
    dbHandler.getBioSampleId(this.sourceId)
  }

  override def checkConsistency(): Boolean = {
    if(this.sourceId != null) true else false
  }

  override def checkDependenciesSatisfaction(table: Table): Boolean = {
    try {
      table match {
        case bioSample: BioSample => {
          if (bioSample.types != null && bioSample.types.equals("tissue") && bioSample.tissue == null) {
            Statistics.constraintsViolated += 1
            this.logger.warn("Biosample tissue constraints violated")
            false
          }
          else if (bioSample.types != null && bioSample.types.equals("cellLine") && bioSample.tissue == null) {
            Statistics.constraintsViolated += 1
            this.logger.warn("Biosample cellLine constraints violated")
            false
          }
/*          else if (bioSample.isHealthy != null && bioSample.isHealthy.getOrElse(false) && bioSample.disease != null) {
            Statistics.constraintsViolated += 1
            this.logger.warn("Biosample isHealty constraints violated")
            false
          }*/
          else
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

  def convertTo(values: Seq[(Int, String, Option[String], Option[String], Option[String], Option[Boolean], Option[String])]): Unit = {
    if(values.length > 1)
      logger.error(s"Too many value: ${values.length}")
    else {
      var value = values.head
      this.donorId = value._1
      this.sourceId = value._2
      if(value._3.isDefined) this.types = value._3.get
      if(value._4.isDefined) this.tissue = value._4.get
      if(value._5.isDefined) this.cellLine = value._5.get
      if(value._6.isDefined) this.isHealthy = Option(value._6.get) //check if correct putting Option around value._6.get
      if(value._7.isDefined) this.disease = Option(value._7.get)
    }
  }

  def writeInFile(path: String, biologicalReplicateNum: String = ""): Unit = {
    val write = getWriter(path)
    val tableName = "biosample"
    write.append(getMessage(tableName, "biosample_source_id", this.sourceId))
    if(this.types != null) write.append(getMessage(tableName, "type", this.types))
    if(this.tissue != null) write.append(getMessage(tableName, "tissue", this.tissue))
    if(this.cellLine != null) write.append(getMessage(tableName, "cell_line", this.cellLine))
    write.append(getMessage(tableName, "is_healthy", this.isHealthy))
    if(this.disease != null) write.append(getMessage(tableName, "disease", this.disease))
    flushAndClose(write)
  }

}
