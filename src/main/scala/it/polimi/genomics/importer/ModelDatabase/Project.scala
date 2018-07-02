package it.polimi.genomics.importer.ModelDatabase


trait Project extends Table{

  var projectName: String = _

  var programName: String = _

  override def insert(): Int =  {
    dbHandler.insertProject(this.projectName.toUpperCase(),this.programName)
  }

  override def update(): Int =  {
    dbHandler.updateProject(this.projectName.toUpperCase(),this.programName)
  }

  override def updateById(): Unit =  {
    dbHandler.updateProjectById(this.primaryKey, this.projectName.toUpperCase(),this.programName)
  }

  override def setForeignKeys(table: Table): Unit = {
  }

  override def checkInsert(): Boolean ={
    dbHandler.checkInsertProject(this.projectName.toUpperCase())
  }

  override def getId(): Int = {
    dbHandler.getProjectId(this.projectName)
  }

  override def checkConsistency(): Boolean = {
    if(this.projectName != null) true else false
  }

  def convertTo(values: Seq[(String, Option[String])]): Unit = {
    if(values.length > 1)
      logger.error(s"Too many value: ${values.length}")
    else {
      var value = values.head
      this.projectName = value._1
      if(value._2.isDefined) this.programName = value._2.get
    }
  }

  def writeInFile(path: String): Unit = {
    val write = getWriter(path)
    val tableName = "project"
    write.append(getMessage(tableName, "project_name", this.projectName))
    if(this.programName != null) write.append(getMessage(tableName, "program_name", this.programName))
    flushAndClose(write)
  }
}
