package it.polimi.genomics.importer.ModelDatabase

import com.typesafe.config.ConfigFactory
import it.polimi.genomics.importer.ModelDatabase.Utils.Statistics
import it.polimi.genomics.importer.RemoteDatabase.DbHandler
import org.apache.log4j.Logger

import scala.util.matching.Regex

class FromDbToTsv() {
  private val conf = ConfigFactory.load()


  var donor: Donor = _
  var bioSample: BioSample = _
  var replicate: Replicate =  _
  var cases: Case = _
  var experimentType: ExperimentType = _
  var container: Container =_
  var project: Project = _
  var item: Item = _
  private val isNewFile = conf.getBoolean("export.newfile")
  private val extension = conf.getString("export.extension")


  val logger: Logger = Logger.getLogger(this.getClass)




  def setTable(donor: Donor, bioSample: BioSample, replicate: Replicate, cases: Case, container: Container, experimentType: ExperimentType, project: Project, item: Item): Unit ={
    this.donor = donor
    this.bioSample = bioSample
    this.replicate =  replicate
    this.cases = cases
    this.experimentType = experimentType
    this.container = container
    this.project = project
    this.item = item
  }


  def run(oldPath: String, regex: Regex): Unit = {
    val path = if(isNewFile) regex.replaceAllIn(oldPath, extension) else oldPath
    logger.info(s"Start to read ${path} file")
    try {
      val sourceIdItem = path.split('/').last.split('.')(0)

      Statistics.tsvFile += 1

      item.convertTo(DbHandler.getItemBySourceId(sourceIdItem))
      item.writeInFile(path)

      experimentType.convertTo(DbHandler.getExperimentTypeById(item.experimentTypeId))
      experimentType.writeInFile(path)

      cases.convertTo(DbHandler.getCaseByItemId(item.primaryKey))
      cases.writeInFile(path)

      container.convertTo(DbHandler.getContainerById(cases.containerId))
      container.writeInFile(path)

      project.convertTo(DbHandler.getProjectById(container.projectId))
      project.writeInFile(path)

      replicate.convertTo(DbHandler.getReplicateByItemId(item.primaryKey))
      replicate.writeInFile(path)

      replicate.getReplicateIdList().foreach(bioSampleId => {
        bioSample.convertTo(DbHandler.getBiosampleById(bioSampleId))
        bioSample.writeInFile(path)

        donor.convertTo(DbHandler.getDonorById(bioSample.donorId))
        donor.writeInFile(path)
      })


      this.recursiveGetItemsByDerivedFromId(this.item.primaryKey)
      if (!this.sourceIdDerivedFrom.equals(""))
        item.writeDerivedFrom(path, this.sourceIdDerivedFrom)
    } catch {
      case e: Exception => logger.error(s"Some error in FromDbToTsv process, go to next file")
    }

  }

  val sourceIdDerivedFrom: String = ""
  def recursiveGetItemsByDerivedFromId(id: Int): Unit ={
    DbHandler.getItemsByDerivedFromId(id).foreach(item => {
      sourceIdDerivedFrom.concat(" " + item._3)
      this.recursiveGetItemsByDerivedFromId(item._1)
    })
  }

}
