package it.polimi.genomics.importer.ModelDatabase

import it.polimi.genomics.importer.ModelDatabase.TCGA.Table._
import it.polimi.genomics.importer.ModelDatabase.Utils.Statistics
import it.polimi.genomics.importer.RemoteDatabase.DbHandler
import org.slf4j.{Logger, LoggerFactory}

class FromDbToTsvTCGA (path: String){


  Statistics.tsvFile += 1
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  logger.info(s"Start to read ${sourceIdItem}")
  val sourceIdItem = path.split('/').last.split('.')(0)

  println(sourceIdItem)

  var itemTCGA: ItemTCGA = new ItemTCGA

  println(DbHandler.getItemBySourceId(sourceIdItem))

  itemTCGA.convertTo(DbHandler.getItemBySourceId(sourceIdItem))

  itemTCGA.writeInFile(path)

  val containerTCGA: ContainerTCGA = new ContainerTCGA
  containerTCGA.convertTo(DbHandler.getContainerById(itemTCGA.containerId))
  containerTCGA.writeInFile(path)

  val experimentTypeTCGA: ExperimentTypeTCGA = new ExperimentTypeTCGA
  experimentTypeTCGA.convertTo(DbHandler.getExperimentTypeById(containerTCGA.experimentTypeId))
  experimentTypeTCGA.writeInFile(path)

  val caseTCGA: CaseTCGA = new CaseTCGA
  caseTCGA.convertTo(DbHandler.getCaseByItemId(itemTCGA.primaryKey))
  caseTCGA.writeInFile(path)

  val projectTCGA: ProjectTCGA = new ProjectTCGA
  projectTCGA.convertTo(DbHandler.getProjectById(caseTCGA.projectId))
  projectTCGA.writeInFile(path)

  val replicateTCGA: ReplicateTCGA = new ReplicateTCGA
  replicateTCGA.convertTo(DbHandler.getReplicateByItemId(replicateTCGA.primaryKey))
  replicateTCGA.writeInFile(path)


  val bioSampleTCGA: BioSampleTCGA = new BioSampleTCGA
  bioSampleTCGA.convertTo(DbHandler.getBiosampleById(replicateTCGA.primaryKey))
  bioSampleTCGA.writeInFile(path)

  val donorTCGA: DonorTCGA= new DonorTCGA
  donorTCGA.convertTo(DbHandler.getDonorById(bioSampleTCGA.donorId))
  donorTCGA.writeInFile(path)

}
