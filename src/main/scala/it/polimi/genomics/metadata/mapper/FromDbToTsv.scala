package it.polimi.genomics.metadata.mapper

import java.io.File

import com.typesafe.config.ConfigFactory
import it.polimi.genomics.metadata.mapper.Utils.Statistics
import it.polimi.genomics.metadata.mapper.RemoteDatabase.DbHandler
import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger

import scala.util.matching.Regex

class FromDbToTsv() {
  private val conf = ConfigFactory.load()


  var donor: Donor = _
  var bioSample: BioSample = _
  var replicate: Replicate =  _
  var cases: Case = _
  var experimentType: ExperimentType = _
  var dataset: Dataset =_
  var project: Project = _
  var item: Item = _
  private val isNewFile = conf.getBoolean("export.newfile")
  private val extension = conf.getString("export.extension")


  val logger: Logger = Logger.getLogger(this.getClass)




  def setTable(donor: Donor, bioSample: BioSample, replicate: Replicate, cases: Case, dataset: Dataset, experimentType: ExperimentType, project: Project, item: Item): Unit ={
    this.donor = donor
    this.bioSample = bioSample
    this.replicate =  replicate
    this.cases = cases
    this.experimentType = experimentType
    this.dataset = dataset
    this.project = project
    this.item = item
  }


 /* def run(oldPath: String, regex: Regex): Unit = {
    val path = if(isNewFile) regex.replaceAllIn(oldPath, extension) else oldPath
    if(isNewFile) FileUtils.deleteQuietly(new File(path))
    logger.info(s"Start to read ${oldPath} file")
    try {
      val sourceIdItem = path.split('/').last.split('.')(0)

      Statistics.tsvFile += 1

      item.convertTo(DbHandler.getItemBySourceId(sourceIdItem))
      item.writeInFile(path)

      experimentType.convertTo(DbHandler.getExperimentTypeById(item.experimentTypeId))
      experimentType.writeInFile(path)

      cases.convertTo(DbHandler.getCaseByItemId(item.primaryKey))
      cases.writeInFile(path)

      dataset.convertTo(DbHandler.getDatasetById(item.datasetId))
      dataset.writeInFile(path)

      project.convertTo(DbHandler.getProjectById(cases.projectId))
      project.writeInFile(path)

      replicate.convertTo(DbHandler.getReplicateByItemId(item.primaryKey))
      replicate.writeInFile(path)

      replicate.getReplicateIdList().foreach(bioSampleId => {
        val biosampleReplicateNum = replicate.getBiosampleNum(bioSampleId)
        bioSample.convertTo(DbHandler.getBiosampleById(bioSampleId))
        bioSample.writeInFile(path, biosampleReplicateNum)

        donor.convertTo(DbHandler.getDonorById(bioSample.donorId))
        donor.writeInFile(path, biosampleReplicateNum)
      })


      this.recursiveGetItemsByDerivedFromId(this.item.primaryKey)
      if (!this.sourceIdDerivedFrom.equals(""))
        item.writeDerivedFrom(path, this.sourceIdDerivedFrom)

      logger.info(s"File ${oldPath} correctly exported")
      Statistics.correctExportedFile += 1

    } catch {
      case e: Exception => {
        Statistics.errorExportedFile += 1
        logger.error(s"Some error in FromDbToTsv process, go to next file")
      }
    }

  }

  var sourceIdDerivedFrom: String = ""
  def recursiveGetItemsByDerivedFromId(id: Int): Unit = {
    DbHandler.getItemsByDerivedFromId(id).foreach(item => {
      if(item._3 != null) {
        sourceIdDerivedFrom = sourceIdDerivedFrom.concat(item._3 + " ")
        this.recursiveGetItemsByDerivedFromId(item._1)
      }
    })
  }
  */

}
