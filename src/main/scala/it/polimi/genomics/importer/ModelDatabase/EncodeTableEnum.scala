package it.polimi.genomics.importer.ModelDatabase

import exceptions.NoTableNameException

object EncodeTableEnum extends Enumeration{

    val Donors = Value("DONORS")
    val BioSamples = Value("BIOSAMPLES")
    val Replicates = Value("REPLICATES")
    val ExperimentsType = Value("EXPERIMENTSTYPE")
    val Projects = Value("PROJECTS")
    val Containers = Value("CONTAINERS")
    val Cases = Value("CASES")
    val Items = Value("ITEMS")
    val CasesItems = Value("CASESITEMS")

    def getTable(value: Value): EncodeTable ={
      value match {
        case Donors => return new Donor
        case BioSamples => return new BioSample
        case Replicates => return new Replicate
        case ExperimentsType => return  new ExperimentType
        case Projects => return new Project
        case Containers => return new Container
        case Cases => return new Case
        case Items => return new Item
        case CasesItems => return new CaseItem
        case _ => throw new NoTableNameException(value.toString)
      }
    }

  def getOrderOfInsertion(): List[Value] ={
    return List(Donors,BioSamples,Replicates,ExperimentsType,Projects,Containers,Cases,Items,CasesItems)
  }

}
