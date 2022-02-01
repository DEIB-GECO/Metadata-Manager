package it.polimi.genomics.metadata.mapper.GWAS

import exceptions.NoTableNameException
import it.polimi.genomics.metadata.mapper.GWAS.Table.{AncestryGwas, BioSampleGwas, CaseGwas, CaseItemGwas, CohortGwas, DatasetGwas, DonorGwas, ExperimentTypeGwas, GwasTable, ItemGwas, ProjectGwas, ReplicateGwas, ReplicateItemGwas}
import it.polimi.genomics.metadata.mapper.{Ancestry, Case, Cohort, Dataset, ExperimentType, Item, Pair, Project, Table, Tables}
import org.apache.log4j.Logger

class GwasTables(gwasTableId: GwasTableId) extends Tables {

  this.logger = Logger.getLogger(this.getClass)

  def setPathOnTables(): Unit = {
    getOrderOfInsertion().map(t => this.selectTableByValue(t).filePath_:(this._filePath))
  }

  def getNewTable(value: Value): Table = {
    value match {
      case Ancestries => new AncestryGwas(gwasTableId, gwasTableId.ancestryQuantity)
      case Cohorts => new CohortGwas(gwasTableId)
      case ExperimentsType => new ExperimentTypeGwas(gwasTableId)
      case Projects => new ProjectGwas(gwasTableId)
      case Datasets => new DatasetGwas(gwasTableId)
      case Cases => new CaseGwas(gwasTableId)
      case Items => new ItemGwas(gwasTableId)
      case CasesItems => new CaseItemGwas(gwasTableId)
      //the following will be empty for Gwas
      case Donors => new DonorGwas(gwasTableId)
      case BioSamples => new BioSampleGwas(gwasTableId)
      case Replicates => new ReplicateGwas(gwasTableId)
      case ReplicatesItems => new ReplicateItemGwas(gwasTableId)
      case Pairs => return new Pair
      case _ => throw new NoTableNameException(value.toString)
    }
  }


  def getListOfTables(): (Ancestry, Case, Dataset, ExperimentType, Project, Item, Cohort) = {
    return (new AncestryGwas(gwasTableId, gwasTableId.ancestryQuantity), new CaseGwas(gwasTableId), new DatasetGwas(gwasTableId), new ExperimentTypeGwas(gwasTableId), new ProjectGwas(gwasTableId), new ItemGwas(gwasTableId), new CohortGwas(gwasTableId))
  }

  def nextPosition(tableName: String, globalKey: String, method: String): Unit = {
    val gwasTable: GwasTable =  this.selectTableByName(tableName).asInstanceOf[GwasTable]
    gwasTable.nextPosition(globalKey, method)
  }

}
