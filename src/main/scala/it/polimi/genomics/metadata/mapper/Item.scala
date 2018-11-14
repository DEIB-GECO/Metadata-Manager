package it.polimi.genomics.metadata.mapper

import it.polimi.genomics.metadata.mapper.Exception.NoTupleInDatabaseException
import it.polimi.genomics.metadata.mapper.Utils.Statistics


trait Item extends Table{

  var experimentTypeId : Int = _

  var datasetId: Int = _

  var sourceId : String = _

//  var dataType: String = _

//  var format : String = _

  var size : Long = _

  var date : String = _

  var checksum : String = _

  var pipeline : String = _

  var platform : String =_

  var sourceUrl : String = _

  var localUrl : String = _

  _hasForeignKeys = true

  _foreignKeysTables = List("EXPERIMENTSTYPE", "DATASETS")

  _hasDependencies = true

  _dependenciesTables = List("ITEMS")


  override def insert(): Int = {
    val id = dbHandler.insertItem(experimentTypeId,datasetId,this.sourceId,this.size,this.date,this.checksum,this.platform,this.pipeline,this.sourceUrl,this.localUrl)
    Statistics.itemInserted += 1
    id
  }

  override def update(): Int = {
    val id = dbHandler.updateItem(experimentTypeId,datasetId,this.sourceId,this.size,this.date,this.checksum,this.platform,this.pipeline,this.sourceUrl,this.localUrl)
    Statistics.itemUpdated += 1
    id
  }

  override def updateById(): Unit = {
    val id = dbHandler.updateItemById(this.primaryKey, experimentTypeId,datasetId,this.sourceId,this.size,this.date,this.checksum,this.platform,this.pipeline,this.sourceUrl,this.localUrl)
    Statistics.itemUpdated += 1
  }

  override def setForeignKeys(table: Table): Unit = {
    if(table.isInstanceOf[ExperimentType])
      this.experimentTypeId = table.primaryKey
    if(table.isInstanceOf[Dataset]) {
      this.datasetId = table.primaryKey
    }
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

//  override def checkDependenciesSatisfaction(table: Table): Boolean = {
//    try {
//      table match {
//        case item: Item => {
//          if (item.format.equals("fastq") && this.platform == null) {
//            Statistics.constraintsViolated += 1
//            this.logger.warn("Item format platform constrains violated")
//            false
//          } else if (item.format.equals("bam") && this.pipeline == null) {
//            Statistics.constraintsViolated += 1
//            this.logger.warn("Item format pipeline constrains violated")
//            false
//          } else if (item.dataType.equals("reads") && this.platform == null) {
//            Statistics.constraintsViolated += 1
//            this.logger.warn("Item dataType platform constrains violated")
//            false
//          }
//          else
//            true
//        }
//        case _ => true
//      }
//    }catch{
//      case e: Exception => {
//        logger.warn("java.lang.NullPointerException")
//        true
//      };
//    }
//  }

  def convertTo(values: Seq[(Int, Int, Int, String, Option[Long], Option[String], Option[String], Option[String], Option[String])]): Unit = {
    try {
      this.checkValueLength(values)
      if (values.length > 1)
        logger.error(s"Too many value: ${values.length}")
      else {
        var value = values.head
        this.primaryKey_(value._1)
        this.experimentTypeId = value._2
        this.datasetId = value._3
        this.sourceId = value._4
        if (value._5.isDefined) this.size = value._5.get
        if (value._6.isDefined) this.pipeline = value._6.get
        if (value._7.isDefined) this.platform = value._7.get
        if (value._8.isDefined) this.sourceUrl = value._8.get
        if (value._9.isDefined) this.localUrl = value._9.get
      }
    } catch {
      case notTuple: NoTupleInDatabaseException => logger.error(s"Item: ${notTuple.message}")
    }
  }

  def writeInFile(path: String): Unit = {
    val write = getWriter(path)
    val tableName = "item"
    write.append(getMessage(tableName, "item_source_id", this.sourceId))

    if(this.size != 0) write.append(getMessage(tableName, "size", this.size))
    if(this.pipeline != null) write.append(getMessage(tableName, "pipeline", this.pipeline))
    if(this.platform != null) write.append(getMessage(tableName, "platform", this.platform))
    if(this.sourceUrl != null) write.append(getMessage(tableName, "source_url", this.sourceUrl))
    if(this.localUrl != null) write.append(getMessage(tableName, "source_url", this.localUrl))
    flushAndClose(write)
  }

  def writeDerivedFrom(path: String, derived: String): Unit = {
    val write = getWriter(path)
    val tableName = "item"
    write.append(getMessage(tableName ,"derived_from", derived))
    flushAndClose(write)

  }
}
