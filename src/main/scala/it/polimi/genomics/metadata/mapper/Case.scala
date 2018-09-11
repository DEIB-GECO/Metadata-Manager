package it.polimi.genomics.metadata.mapper


trait Case extends Table{

  var projectId : Int = _

  var sourceId : String = _

  var sourceSite : String = _

  var externalRef: String = _

  _hasForeignKeys = true

  _foreignKeysTables = List("PROJECTS")

  override def insert(): Int = {
    dbHandler.insertCase(this.projectId,this.sourceId,this.sourceSite,this.externalRef)
  }

  override def update(): Int = {
    dbHandler.updateCase(this.projectId,this.sourceId,this.sourceSite,this.externalRef)
  }

  override def updateById(): Unit = {
    dbHandler.updateCaseById(this.primaryKey, this.projectId,this.sourceId,this.sourceSite,this.externalRef)
  }

  override def setForeignKeys(table: Table): Unit = {
    this.projectId = table.primaryKey
  }

  override def checkInsert(): Boolean ={
    dbHandler.checkInsertCase(this.sourceId)
  }

  override def getId(): Int = {
    dbHandler.getCaseId(this.sourceId)
  }

  override def checkConsistency(): Boolean = {
    if(this.sourceId != null) true else false
  }

  def convertTo(values: Seq[(Int, String, Option[String], Option[String])]): Unit = {
    if (values.length > 1)
      logger.error(s"Too many value: ${values.length}")
    else {
      var value = values.head
      this.projectId = value._1
      this.sourceId = value._2
      if (value._3.isDefined) this.sourceSite = value._3.get
      if (value._4.isDefined) this.externalRef = value._4.get
    }
  }

  def writeInFile(path: String): Unit = {
    val write = getWriter(path)
    val tableName = "case_study"
    write.append(getMessage(tableName, "source_id", this.sourceId))
    if(this.sourceSite != null) write.append(getMessage(tableName, "source_site", this.sourceSite))
    if(this.externalRef != null) write.append(getMessage(tableName, "external_ref", this.externalRef))
    flushAndClose(write)
  }

}
