package it.polimi.genomics.importer.ModelDatabase


import exceptions.NoTableNameException


class EncodeTables extends Tables{

 // private var replicatesList = new ListBuffer[Replicate]()

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

  //private val tables = collection.mutable.Map[String, AbstractTable]()
  //EncodeTableEnum.values.foreach(v => tables += v ->EncodeTableEnum.getTable(v))

  this.values.foreach(v => tables += v -> this.getNewTable(v))
  //override def selectTableByName(name: String): Table = tables(EncodeTableEnum.withName(name))
  //override def selectTableByValue(enum: EncodeTableEnum.Value): Table = tables(enum)

  override def selectTableByName(name: String): Table = tables(this.withName(name))
  override def selectTableByValue(enum: this.Value): Table = tables(enum)

  def getNewTable(value: Value): EncodeTable = {
    value match {
      case Donors => return new Donor
      case BioSamples => return new BioSample
      case Replicates => return new Replicate
      case ExperimentsType => return new ExperimentType
      case Projects => return new Project
      case Containers => return new Container
      case Cases => return new Case
      case Items => return new Item
      case CasesItems => return new CaseItem
      case ReplicatesItems => return new ReplicateItem
      case _ => throw new NoTableNameException(value.toString)
    }
  }

  def insertTables(): Unit ={
    getOrderOfInsertion().map(t => this.selectTableByValue(t)).foreach(table =>{
      if(table.hasForeignKeys){
        table.foreignKeysTables.map(t => this.selectTableByName(t)).map(t => table.setForeignKeys(t))
      }
      table.insertRow()
    }
    )
  }

  def getOrderOfInsertion(): List[Value] ={
    return List(Donors,BioSamples,Replicates,ExperimentsType,Projects,Containers,Cases,Items,CasesItems,ReplicatesItems)
  }
}
