package it.polimi.genomics.metadata.mapper.Encode

import exceptions.NoTableNameException
import it.polimi.genomics.metadata.mapper.Encode.Table._
import it.polimi.genomics.metadata.mapper.Utils.Statistics
import it.polimi.genomics.metadata.mapper.{BioSample, Case, Dataset, Donor, ExperimentType, Item, Project, Replicate, Pair, Table, Tables}
import org.apache.log4j.Logger


class EncodeTables(encodeTableId: EncodeTableId) extends Tables{

  /*protected var _filePath: String = _

  def filePath: String = _filePath
  def filePath_: (filePath: String): Unit = this._filePath = filePath*/

  //val logger: Logger = Logger.getLogger(this.getClass)

  this.logger = Logger.getLogger(this.getClass)

  //this.values.foreach(v => tables += v -> this.getNewTable(v))
  //override def selectTableByName(name: String): Table = tables(EncodeTableEnum.withName(name))
  //override def selectTableByValue(enum: EncodeTableEnum.Value): Table = tables(enum)

  /*override def selectTableByName(name: String): Table = tables(this.withName(name))
  override def selectTableByValue(enum: this.Value): Table = tables(enum)*/

  def setPathOnTables(): Unit = {
    getOrderOfInsertion().map(t => this.selectTableByValue(t).filePath_:(this._filePath))
  }

  def getNewTable(value: Value): Table = {
    value match {
      case Donors => return new DonorEncode(encodeTableId, encodeTableId.bioSampleQuantity)
      case BioSamples => return new BioSampleEncode(encodeTableId, encodeTableId.bioSampleQuantity)
      case Replicates => return new ReplicateEncode(encodeTableId)
      case ExperimentsType => return new ExperimentTypeEncode(encodeTableId)
      case Projects => return new ProjectEncode(encodeTableId)
      case Datasets => return new DatasetEncode(encodeTableId)
      case Cases => return new CaseEncode(encodeTableId)
      case Items => return new ItemEncode(encodeTableId)
      case CasesItems => return new CaseItemEncode(encodeTableId)
      case ReplicatesItems => return new ReplicateItemEncode(encodeTableId)
      case Pairs => return new Pair
      case _ => throw new NoTableNameException(value.toString)
    }
  }

  /*override def insertTables(): Unit ={
    var insert = true
    getOrderOfInsertion().map(t => this.selectTableByValue(t)).foreach(table =>{
      if(table.hasForeignKeys){
        table.foreignKeysTables.map(t => this.selectTableByName(t)).map(t => table.setForeignKeys(t))
      }
      if(table.checkConsistency() == false && insert) {
        insert = false
        logger.warn(s"Primary key of $table doesn't find")
        Statistics.releasedItemNotInserted += 1
      }
      if(insert) {
        table.insertRow()
      }
    })
  }*/

   def getListOfTables(): (Donor, BioSample, Replicate, Case, Dataset, ExperimentType, Project, Item) = {
    val encodeTableId: EncodeTableId = new EncodeTableId
    return (new DonorEncode(encodeTableId,1), new BioSampleEncode(encodeTableId, 1), new ReplicateEncode(encodeTableId), new CaseEncode(encodeTableId),
    new DatasetEncode(encodeTableId), new ExperimentTypeEncode(encodeTableId), new ProjectEncode(encodeTableId), new ItemEncode(encodeTableId))
  }

  def nextPosition(tableName: String, globalKey: String, method: String): Unit = {
    val encodeTable: EncodeTable =  this.selectTableByName(tableName).asInstanceOf[EncodeTable]
    encodeTable.nextPosition(globalKey, method)
  }


}
