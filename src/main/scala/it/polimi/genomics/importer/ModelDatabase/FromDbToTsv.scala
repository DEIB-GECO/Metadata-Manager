package it.polimi.genomics.importer.ModelDatabase

import it.polimi.genomics.importer.ModelDatabase.Encode.EncodeTableId
import it.polimi.genomics.importer.ModelDatabase.Encode.Table._
import it.polimi.genomics.importer.ModelDatabase.Utils.Statistics
import it.polimi.genomics.importer.RemoteDatabase.DbHandler
import org.slf4j.{Logger, LoggerFactory}

class FromDbToTsv(path: String) {

  val encodeTableId: EncodeTableId = new EncodeTableId
  Statistics.tsvFile += 1
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  logger.info(s"Start to read ${sourceIdItem}")
  val sourceIdItem = path.split('/').last.split('.')(0)

  println(sourceIdItem)

  var itemEncode: ItemEncode = new ItemEncode(encodeTableId)

  itemEncode.convertTo(DbHandler.getItemBySourceId(sourceIdItem))

  itemEncode.writeInFile(path)

  val containerEncode: ContainerEncode = new ContainerEncode(encodeTableId)
  containerEncode.convertTo(DbHandler.getContainerById(itemEncode.containerId))
  containerEncode.writeInFile(path)

  val experimentTypeEncode: ExperimentTypeEncode = new ExperimentTypeEncode(encodeTableId)
  experimentTypeEncode.convertTo(DbHandler.getExperimentTypeById(containerEncode.experimentTypeId))
  experimentTypeEncode.writeInFile(path)

  val caseEncode: CaseEncode = new CaseEncode(encodeTableId)
  caseEncode.convertTo(DbHandler.getCaseByItemId(itemEncode.primaryKey))
  caseEncode.writeInFile(path)

  val projectEncode: ProjectEncode = new ProjectEncode(encodeTableId)
  projectEncode.convertTo(DbHandler.getProjectById(caseEncode.projectId))
  projectEncode.writeInFile(path)

  val replicateEncode: ReplicateEncode = new ReplicateEncode(encodeTableId)
  replicateEncode.convertTo(DbHandler.getReplicateByItemId(itemEncode.primaryKey))
  replicateEncode.writeInFile(path)


  replicateEncode.bioSampleIdList.foreach(bioSampleId => {
    val bioSampleEncode: BioSampleEncode = new BioSampleEncode(encodeTableId, 1)
    bioSampleEncode.convertTo(DbHandler.getBiosampleById(bioSampleId))
    bioSampleEncode.writeInFile(path)

    val donorEncode: DonorEncode = new DonorEncode(encodeTableId,1)
    donorEncode.convertTo(DbHandler.getDonorById(bioSampleEncode.donorId))
    donorEncode.writeInFile(path)
  })

  val sourceIdDerivedFrom: String = ""
  this.recursiveGetItemsByDerivedFromId(this.itemEncode.primaryKey)
  def recursiveGetItemsByDerivedFromId(id: Int): Unit ={
    DbHandler.getItemsByDerivedFromId(id).foreach(item => {
      sourceIdDerivedFrom.concat(" " + item._3)
      this.recursiveGetItemsByDerivedFromId(item._1)
    })
  }




}
