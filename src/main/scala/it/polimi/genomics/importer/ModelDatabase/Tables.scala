package it.polimi.genomics.importer.ModelDatabase

import com.typesafe.config.ConfigFactory
import it.polimi.genomics.importer.ModelDatabase.Utils.Statistics
import org.apache.log4j.Logger

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

  var logger: Logger = _

  //def selectTableByName(name: String): Table
  //def selectTableByValue(enum: this.Value): Table
  def selectTableByName(name: String): Table = tables(this.withName(name))
  def selectTableByValue(enum: this.Value): Table = tables(enum)

  def getOrderOfInsertion(): List[Value] ={
    return List(Donors,BioSamples,Replicates,ExperimentsType,Projects,Containers,Cases,Items,CasesItems,ReplicatesItems)
  }

  def insertTables(): Unit = {

    val conf = ConfigFactory.load()
    val constrainsSatisfacted = this.checkTablesConstrainsSatisfaction()
    if(!conf.getBoolean("constraints.activated") || constrainsSatisfacted) {
      var insert = true
      getOrderOfInsertion().map(t => this.selectTableByValue(t)).foreach(table => {
        if (table.hasForeignKeys) {
          table.foreignKeysTables.map(t => this.selectTableByName(t)).map(t => table.setForeignKeys(t))
        }
        if (table.checkConsistency() == false && insert) {
          insert = false
          logger.warn(s"Primary key of $table doesn't find")
          Statistics.releasedItemNotInserted += 1
        }
        if (insert) {
          table.insertRow()
        }
      }
      )
    }
  }

  def checkTablesConstrainsSatisfaction(): Boolean = {
    var res = true
    getOrderOfInsertion().map(tableValue => this.selectTableByValue(tableValue)).foreach(table =>{
      if(table.hasDependencies) {
        table.dependenciesTables.map(t => this.selectTableByName(t)).map(t => if (!table.checkDependenciesSatisfaction(t)) res = false) //non lo torno subito perchè devo controllare le contrains di tutte le tabelle, non solo la prima che non matcha
      }
    })
    res
    }

  this.values.foreach(v => tables += v -> this.getNewTable(v))

  def getNewTable(value: Value): Table

  protected var _filePath: String = _

  def filePath: String = _filePath
  def filePath_: (filePath: String): Unit = this._filePath = filePath

  def getListOfTables(): (Donor, BioSample, Replicate, Case, Container, ExperimentType, Project, Item)

}
