package it.polimi.genomics.metadata.mapper

import com.typesafe.config.ConfigFactory
import it.polimi.genomics.metadata.mapper.Utils.Statistics
import org.apache.log4j.Logger

import scala.collection.mutable


trait Tables extends Enumeration {

  val Donors = Value("DONORS")
  val BioSamples = Value("BIOSAMPLES")
  val Replicates = Value("REPLICATES")
  val ExperimentsType = Value("EXPERIMENTSTYPE")
  val Projects = Value("PROJECTS")
  val Datasets = Value("DATASETS")
  val Cases = Value("CASES")
  val Items = Value("ITEMS")
  val CasesItems = Value("CASESITEMS")
  val ReplicatesItems = Value("REPLICATESITEMS")
  val Pairs = Value("PAIRS")

  //for Gwas
  val Cohorts = Value("COHORTS")
  val Ancestries = Value("ANCESTRIES")


  protected val tables: mutable.Map[Value, Table] = collection.mutable.Map[this.Value, Table]()

  var logger: Logger = _

  def selectTableByName(name: String): Table = tables(this.withName(name))

  def selectTableByValue(enum: this.Value): Table = tables(enum)

  def getOrderOfInsertion(): List[Value] = {
    return List(Donors, BioSamples, Replicates, ExperimentsType, Projects, Datasets, Cases, Items, CasesItems, ReplicatesItems, Cohorts, Ancestries)
  }


  /**
    * Insert the metadata information into the related tables
    *
    * @param states is a map of key -> concatanation of all values
    * @param pairs  is a list of key value pairs
    */
  def insertTables(states: collection.mutable.Map[String, String], pairs: List[(String, String)]): Unit = {

    val conf = ConfigFactory.load()
    //val constraintsSatisfied = this.checkTablesConstraintsSatisfaction()
    val constraintsSatisfied = true //ANNA: for now we don't have constraints
    if (!conf.getBoolean("import.constraints_activated") || constraintsSatisfied) {
      var insert = true
      getOrderOfInsertion().map(t => this.selectTableByValue(t)).foreach { (table: Table) =>
        if (table.hasForeignKeys) {
          table.foreignKeysTables.map(t => this.selectTableByName(t)).map(t => table.setForeignKeys(t))
        }
        if (table.checkConsistency() == false && insert) {
          insert = false
          logger.warn(s"Primary key of $table not found")
          Statistics.releasedItemNotInserted += 1
        }
        if (insert) {
          val insertedId = table.insertRow()
          if (table.isInstanceOf[Item]) {
            logger.info(s"Inserting pairs for item_id $insertedId")
            if (conf.getBoolean("import.import_pairs")) {
              val pairTable: Pair = this.selectTableByName("PAIRS").asInstanceOf[Pair]
              val pairsSet = pairs.toSet
              val oldPairs = pairTable.getPairs(insertedId).toSet

              val itemToInsert = pairsSet.diff(oldPairs)
              val itemToDelete = oldPairs.diff(pairsSet)

           //   logger.info(s"Inserting pairs for item_id $insertedId itemToInsert ${itemToInsert.size}")
           //   logger.info(s"Inserting pairs for item_id $insertedId itemToDelete ${itemToDelete.size}")


           //   logger.info("DELETE")
              if(itemToDelete.nonEmpty) {
                val count = pairTable.deleteBatch(insertedId, itemToDelete.toList)
                //logger.info("DELETED: " + count)
              }
           //   logger.info("INSERT")
              if(itemToInsert.nonEmpty) {
                val count = pairTable.insertBatch(insertedId, itemToInsert.toList)
                //logger.info("INSERTED: " + count)
              }
            }
          }
        }
      }
    }


  }

  def checkTablesConstraintsSatisfaction(): Boolean = {
    var res = true
    getOrderOfInsertion().map(tableValue => this.selectTableByValue(tableValue)).foreach(table => {
      if (table.hasDependencies) {
        table.dependenciesTables.map(t => this.selectTableByName(t)).map(t => if (!table.checkDependenciesSatisfaction(t)) res = false) //non lo torno subito perchè devo controllare le contrains di tutte le tabelle, non solo la prima che non matcha
      }
    })
    res
  }

  this.values.foreach(v => tables += v -> this.getNewTable(v))

  def getNewTable(value: Value): Table

  protected var _filePath: String = _

  def filePath: String = _filePath

  def setFilePath(filePath: String): Unit = this._filePath = filePath

  //def getListOfTables(): (Ancestry, Donor, BioSample, Replicate, Case, Dataset, ExperimentType, Project, Item, Cohort)

}
