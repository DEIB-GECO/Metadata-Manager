package it.polimi.genomics.importer.ModelDatabase

import java.io.{File, FileOutputStream, PrintWriter}

trait BioSample extends Table{

  var donorId: Int = _

  var sourceId: String = _

  var types: String = _

  var tissue: String = _

  var cellLine: String = _

  var isHealty: Boolean = _

  var disease: String = _

  _hasForeignKeys = true

  _foreignKeysTables = List("DONORS")

  _hasDependencies = true

  _dependenciesTables = List("BIOSAMPLE")

  override def insert(): Int ={
    dbHandler.insertBioSample(donorId,this.sourceId,this.types,this.tissue,this.cellLine,this.isHealty,this.disease)
  }

  override def update(): Int = {
    dbHandler.updateBioSample(donorId,this.sourceId,this.types,this.tissue,this.cellLine,this.isHealty,this.disease)
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

  def convertTo(values: Seq[(Int, String, Option[String], Option[String], Option[String], Boolean, Option[String])]): Unit = {
    if(values.length > 1)
      logger.error(s"Too many value: ${values.length}")
    else {
      var value = values.head
      this.donorId = value._1
      this.sourceId = value._2
      if(value._3.isDefined) this.types = value._3.get
      if(value._4.isDefined) this.tissue = value._4.get
      if(value._5.isDefined) this.cellLine = value._5.get
      this.isHealty = value._6
      if(value._7.isDefined) this.disease = value._7.get
    }
  }

  def writeInFile(path: String): Unit = {
    val write = getWriter(path)
    val tableName = "biosample"
    write.append(getMessage(tableName + "__source_id", this.sourceId))
    if(this.types != null) write.append(getMessage(tableName + "__types", this.types))
    if(this.tissue != null) write.append(getMessage(tableName + "__tissue", this.tissue))
    if(this.cellLine != null) write.append(getMessage(tableName + "__cell_line", this.cellLine))
    write.append(getMessage(tableName + "__is_healty", this.isHealty))
    if(this.disease != null) write.append(getMessage(tableName + "__disease", this.disease))
    flushAndClose(write)
  }

}
