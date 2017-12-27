package it.polimi.genomics.importer.ModelDatabase

import scala.collection.mutable


trait Tables extends Enumeration{

  val Donors = Value("DONORS")
  val BioSamples = Value("BIOSAMPLES")
  val Replicates = Value("REPLICATES")
  val ExperimentsType = Value("EXPERIMENTSTYPE")
  val Projects = Value("PROJECTS")
  val Containers = Value("CONTAINERS")
  val Cases = Value("CASES")
  val Items = Value("ITEMS")
  val CasesItems = Value("CASESITEMS")
  val ReplicatesItems = Value ("REPLICATESITEMS")

  protected val tables: mutable.Map[Value, Table] = collection.mutable.Map[this.Value, Table]()

  //def selectTableByName(name: String): Table
  //def selectTableByValue(enum: this.Value): Table
  def selectTableByName(name: String): Table = tables(this.withName(name))
  def selectTableByValue(enum: this.Value): Table = tables(enum)

  def getOrderOfInsertion(): List[Value] ={
    return List(Donors,BioSamples,Replicates,ExperimentsType,Projects,Containers,Cases,Items,CasesItems,ReplicatesItems)
  }

  def insertTables(): Unit

  def getNewTable(value: Value): Table
}
