package it.polimi.genomics.importer.ModelDatabase

trait ExperimentType extends Table{

  var technique : String = _

  var feature : String = _

  var target : String = _

  var antibody : String = _

  override def checkInsert(): Boolean = {
    dbHandler.checkInsertExperimentType(this.technique, this.feature, this.target)
  }

  override def insert(): Int = {
    dbHandler.insertExperimentType(this.technique,this.feature,this.target,this.antibody)
  }

  override def update(): Int = {
    dbHandler.updateExperimentType(this.technique,this.feature,this.target,this.antibody)
  }

  override def updateById(): Unit = {
    dbHandler.updateExperimentTypeById(this.primaryKey, this.technique,this.feature,this.target,this.antibody)
  }

  override def checkConsistency(): Boolean = {
    if(this.technique != null || this.target != null || this.feature != null) true else false
  }

  override def setForeignKeys(table: Table): Unit = {

  }

  override def getId(): Int = {
    dbHandler.getExperimentTypeId(this.technique, this.feature, this.target)
  }

  def convertTo(values: Seq[(Int, Option[String], Option[String], Option[String], Option[String])]): Unit = {
    if(values.length > 1)
      logger.error(s"Too many value: ${values.length}")
    else {
      var value = values.head
      this.primaryKey_(value._1)
      if(value._2.isDefined) this.technique = value._2.get
      if(value._3.isDefined) this.feature = value._3.get
      if(value._4.isDefined) this.target = value._4.get
      if(value._5.isDefined) this.antibody = value._5.get
    }
  }

  def writeInFile(path: String): Unit = {
    val write = getWriter(path)
    val tableName = "experimenttype"

    write.append(getMessage(tableName + "__technique", this.technique))
    if(this.feature != null) write.append(getMessage(tableName + "__feature", this.feature))
    if(this.target != null) write.append(getMessage(tableName + "__age", this.target))
    if(this.antibody != null) write.append(getMessage(tableName + "__antibody", this.antibody))
    flushAndClose(write)
  }

}
