package it.polimi.genomics.importer.ModelDatabase


import exceptions.{NoTableNameException}

class EncodeTables extends Tables{

  val Donors = Value("DONORS")
  val BioSamples = Value("BIOSAMPLES")
  val Replicates = Value("REPLICATES")
  val ExperimentsType = Value("EXPERIMENTSTYPE")
  val Projects = Value("PROJECTS")
  val Containers = Value("CONTAINERS")
  val Cases = Value("CASES")
  val Items = Value("ITEMS")
  val CasesItems = Value("CASESITEMS")

  //private val tables = collection.mutable.Map[String, AbstractTable]()
  //EncodeTableEnum.values.foreach(v => tables += v ->EncodeTableEnum.getTable(v))

  this.values.foreach(v => tables += v -> this.getTable(v))
  //override def selectTableByName(name: String): Table = tables(EncodeTableEnum.withName(name))
  //override def selectTableByValue(enum: EncodeTableEnum.Value): Table = tables(enum)

  override def selectTableByName(name: String): Table = tables(this.withName(name))
  override def selectTableByValue(enum: this.Value): Table = tables(enum)

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

  def insertTables(): Unit ={
    //getOrderOfInsertion().map(table => this.selectTableByValue(table).insert())
    getOrderOfInsertion().map(t => this.selectTableByValue(t)).foreach(table =>{
      if(table.checkInsert()) {
        val id = table.insert()
        table.primaryKey_(id)
      }
      else {
        val id = table.getId
        table.primaryKey_(id)
      }
      if(table.hasForeignKeys){
        table.foreignKeysTables.map(t => this.selectTableByName(t)).map(t => table.setForeignKeys(t))
      }
    }
    )
  }

  def insertDonor(): Unit = {
    this.selectTableByName("DONORS").insert()
  }

  def getOrderOfInsertion(): List[Value] ={
    return List(Donors,BioSamples,Replicates,ExperimentsType,Projects,Containers,Cases,Items,CasesItems)
  }
}
