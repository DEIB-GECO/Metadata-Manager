package it.polimi.genomics.importer.ModelDatabase

import it.polimi.genomics.importer.ModelDatabase.Utils.Statistics


trait Item extends Table{

  var experimentTypeId : Int = _

  var sourceId : String = _

  var dataType: String = _

  var format : String = _

  var size : Long = _

  var pipeline : String = _

  var platform : String =_

  var sourceUrl : String = _

  var localUrl : String = _

  _hasForeignKeys = true

  _foreignKeysTables = List("EXPERIMENTSTYPE")

  _hasDependencies = true

  _dependenciesTables = List("ITEMS")


  override def insert(): Int = {
    val id = dbHandler.insertItem(experimentTypeId,this.sourceId,this.dataType,this.format,this.size,this.platform,this.pipeline,this.sourceUrl,this.localUrl)
    Statistics.itemInserted += 1
    id
  }

  override def update(): Int = {
    val id = dbHandler.updateItem(experimentTypeId,this.sourceId,this.dataType,this.format,this.size,this.platform,this.pipeline,this.sourceUrl,this.localUrl)
    Statistics.itemUpdated += 1
    id
  }

  override def updateById(): Unit = {
    val id = dbHandler.updateItemById(this.primaryKey, experimentTypeId,this.sourceId,this.dataType,this.format,this.size,this.platform,this.pipeline,this.sourceUrl,this.localUrl)
    Statistics.itemUpdated += 1
    return id
  }

  override def setForeignKeys(table: Table): Unit = {
    this.experimentTypeId = table.primaryKey
  }

  override def checkInsert(): Boolean ={
    dbHandler.checkInsertItem(this.sourceId)
  }

  override def getId(): Int = {
    dbHandler.getItemId(this.sourceId)
  }

  override def checkConsistency(): Boolean = {
    if(this.sourceId != null) true else false
  }

  override def checkDependenciesSatisfaction(table: Table): Boolean = {
    try {
      table match {
        case item: Item => {
          if (item.format.equals("fastq") && this.platform == null) {
            Statistics.constraintsViolated += 1
            this.logger.warn("Item format platform constrains violated")
            false
          } else if (item.format.equals("bam") && this.pipeline == null) {
            Statistics.constraintsViolated += 1
            this.logger.warn("Item format pipeline constrains violated")
            false
          } else if (item.dataType.equals("reads") && this.platform == null) {
            Statistics.constraintsViolated += 1
            this.logger.warn("Item dataType platform constrains violated")
            false
          }
          else
            true
        }
        case _ => true
      }
    }catch{
      case e: Exception => {
        logger.warn("java.lang.NullPointerException")
        true
      };
    }
  }

  def convertTo(values: Seq[(Int, Int, String, Option[String], Option[String], Option[Long], Option[String], Option[String], Option[String], Option[String])]): Unit = {
    if(values.length > 1)
      logger.error(s"Too many value: ${values.length}")
    else {
      var value = values.head
      this.primaryKey_(value._1)
      this.experimentTypeId = value._2
      this.sourceId = value._3
      if(value._4.isDefined) this.dataType = value._4.get
      if(value._5.isDefined) this.format = value._5.get
      if(value._6.isDefined) this.size = value._6.get
      if(value._7.isDefined) this.pipeline = value._7.get
      if(value._8.isDefined) this.platform = value._8.get
      if(value._9.isDefined) this.sourceUrl = value._9.get
      if(value._10.isDefined) this.localUrl = value._10.get
    }
  }

  def writeInFile(path: String): Unit = {
    val write = getWriter(path)
    val tableName = "item"
    write.append(getMessage(tableName + "__source_id", this.sourceId))

    if(this.dataType != null) write.append(getMessage(tableName + "__data_type", this.dataType))
    if(this.format != null) write.append(getMessage(tableName + "__format", this.format))
    if(this.size != 0) write.append(getMessage(tableName + "__size", this.size))
    if(this.pipeline != null) write.append(getMessage(tableName + "__pipeline", this.pipeline))
    if(this.platform != null) write.append(getMessage(tableName + "__platform", this.platform))
    if(this.sourceUrl != null) write.append(getMessage(tableName + "__source_url", this.sourceUrl))
    if(this.localUrl != null) write.append(getMessage(tableName + "__local_url", this.localUrl))
    flushAndClose(write)
  }

  def writeDerivedFrom(path: String, derived: String): Unit = {
    val write = getWriter(path)
    val tableName = "item"
    write.append(getMessage(tableName + "_derivedFrom_", derived))
    flushAndClose(write)

  }
}
