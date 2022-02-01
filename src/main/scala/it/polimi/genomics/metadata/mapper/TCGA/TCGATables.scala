package it.polimi.genomics.metadata.mapper.TCGA

import exceptions.NoTableNameException
import it.polimi.genomics.metadata.mapper.TCGA.Table._
import it.polimi.genomics.metadata.mapper.{BioSample, Case, Dataset, Donor, ExperimentType, Item, Pair, Project, Replicate, Table, Tables}
import org.apache.log4j.Logger

class TCGATables extends Tables {

  this.logger = Logger.getLogger(this.getClass)

  def getNewTable(value: Value): Table = {
    value match {
      case Donors => new DonorTCGA
      case BioSamples => new BioSampleTCGA
      case Replicates => new ReplicateTCGA
      case ExperimentsType => new ExperimentTypeTCGA
      case Projects => new ProjectTCGA
      case Datasets => new DatasetTCGA
      case Cases => new CaseTCGA
      case Items => new ItemTCGA
      case CasesItems => new CaseItemTCGA
      case ReplicatesItems => new ReplicateItemTCGA
      case Pairs => return new Pair
      case _ => throw new NoTableNameException(value.toString)
    }
  }

  /*override*/ def getListOfTables(): (Donor, BioSample, Replicate, Case, Dataset, ExperimentType, Project, Item) = {
    return (new DonorTCGA(), new BioSampleTCGA(), new ReplicateTCGA(), new CaseTCGA(),
      new DatasetTCGA(), new ExperimentTypeTCGA(), new ProjectTCGA(), new ItemTCGA())
  }
}
