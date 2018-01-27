package it.polimi.genomics.importer.ModelDatabase.TCGA

import exceptions.NoTableNameException
import it.polimi.genomics.importer.ModelDatabase.Encode.EncodeTableId
import it.polimi.genomics.importer.ModelDatabase.Encode.Table._
import it.polimi.genomics.importer.ModelDatabase.TCGA.Table._
import it.polimi.genomics.importer.ModelDatabase.Utils.Statistics
import it.polimi.genomics.importer.ModelDatabase.{BioSample, Case, Container, Donor, ExperimentType, Item, Project, Replicate, Table, Tables}
import org.apache.log4j.Logger

class TCGATables extends Tables{

  /*protected var _filePath: String = _

  def filePath: String = _filePath
  def filePath_: (filePath: String): Unit = this._filePath = filePath*/

  //val logger: Logger = Logger.getLogger(this.getClass)

  this.logger = Logger.getLogger(this.getClass)


  //this.values.foreach(v => tables += v -> this.getNewTable(v))

  def getNewTable(value: Value): Table = {
    value match {
      case Donors => return new DonorTCGA
      case BioSamples => return new BioSampleTCGA
      case Replicates => return new ReplicateTCGA
      case ExperimentsType => return new ExperimentTypeTCGA
      case Projects => return new ProjectTCGA
      case Containers => return new ContainerTCGA
      case Cases => return new CaseTCGA
      case Items => return new ItemTCGA
      case CasesItems => return new CaseItemTCGA
      case ReplicatesItems => return new ReplicateItemTCGA
      case _ => throw new NoTableNameException(value.toString)
    }
  }

  /*override def insertTables(): Unit = {
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
    }
    )
  }*/

  override def getListOfTables(): (Donor, BioSample, Replicate, Case, Container, ExperimentType, Project, Item) = {
    val encodeTableId: EncodeTableId = new EncodeTableId
    return (new DonorTCGA(), new BioSampleTCGA(), new ReplicateTCGA(), new CaseTCGA(),
      new ContainerTCGA(), new ExperimentTypeTCGA(), new ProjectTCGA(), new ItemTCGA())
  }
}
