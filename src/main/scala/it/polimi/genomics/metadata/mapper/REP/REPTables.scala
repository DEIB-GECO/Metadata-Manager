package it.polimi.genomics.metadata.mapper.REP

import exceptions.NoTableNameException
import it.polimi.genomics.metadata.mapper.REP.Table._
import it.polimi.genomics.metadata.mapper._
import org.apache.log4j.Logger


class REPTables(repTableId: REPTableId) extends Tables{


  this.logger = Logger.getLogger(this.getClass)


  def setPathOnTables(): Unit = {
    getOrderOfInsertion().map(t => this.selectTableByValue(t).filePath_:(this._filePath))
  }

  def getNewTable(value: Value): Table = {
    value match {
      case Donors => return new DonorREP(repTableId, repTableId.bioSampleQuantity)
      case BioSamples => return new BioSampleREP(repTableId, repTableId.bioSampleQuantity)
      case Replicates => return new ReplicateREP(repTableId)
      case ExperimentsType => return new ExperimentTypeREP(repTableId)
      case Projects => return new ProjectREP(repTableId)
      case Datasets => return new DatasetREP(repTableId)
      case Cases => return new CaseREP(repTableId)
      case Items => return new ItemREP(repTableId)
      case CasesItems => return new CaseItemREP(repTableId)
      case ReplicatesItems => return new ReplicateItemREP(repTableId)
      case Pairs => return new Pair
      case _ => throw new NoTableNameException(value.toString)
    }
  }

   def getListOfTables(): (Donor, BioSample, Replicate, Case, Dataset, ExperimentType, Project, Item) = {
    val repTableId: REPTableId = new REPTableId
    return (new DonorREP(repTableId,1), new BioSampleREP(repTableId, 1), new ReplicateREP(repTableId), new CaseREP(repTableId),
    new DatasetREP(repTableId), new ExperimentTypeREP(repTableId), new ProjectREP(repTableId), new ItemREP(repTableId))
  }

  def nextPosition(tableName: String, globalKey: String, method: String): Unit = {
    val repTable: REPTable =  this.selectTableByName(tableName).asInstanceOf[REPTable]
    repTable.nextPosition(globalKey, method)
  }


}
