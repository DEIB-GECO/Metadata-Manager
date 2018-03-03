package it.polimi.genomics.importer.RemoteDatabase

import com.typesafe.config.ConfigFactory
import org.slf4j.{Logger, LoggerFactory}

//import slick.driver.PostgresDriver.api._
import slick.jdbc.PostgresProfile.api._
//import slick.driver.MySQLDriver.api._

import slick.jdbc.meta.MTable
import slick.lifted.Tag

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}





object DbHandler {
  val conf = ConfigFactory.load()

  private val DONOR_TABLE_NAME = "donor"
  private val BIOSAMPLE_TABLE_NAME = "biosample"
  private val REPLICATE_TABLE_NAME = "replicate"
  private val CASE_TABLE_NAME = "case_study"
  private val CONTAINER_TABLE_NAME = "container"
  private val PROJECT_TABLE_NAME = "project"
  private val EXPERIMENTTYPE_TABLE_NAME = "experiment_type"
  private val ITEM_TABLE_NAME = "item"
  private val DERIVEDFROM_TABLE_NAME = "derived_from"
  private val CASEITEM_TABLE_NAME = "case2item"
  private val REPLICATEITEM_TABLE_NAME = "replicate2item"
  private val CASE_TCGA_MAPPING = "case_tcga_mapping"



  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  /*val connectionUrl = "jdbc:postgresql://localhost/gecotest1?user=geco&password=geco78"

   val driver = "org.postgresql.Driver"
   val database = Database.forURL(connectionUrl, driver, keepAliveConnection = true)*/
  val database = Database.forURL(
   conf.getString("database.url"),
   conf.getString("database.username"),
   conf.getString("database.password"),
   driver=conf.getString("database.driver")
  )
  def setDatabase(): Unit = {

    val tables = Await.result(database.run(MTable.getTables), Duration.Inf).toList

    //donors
    logger.info("Start to create the database")

    if (!tables.exists(_.name.name == DONOR_TABLE_NAME)) {
      var queries = DBIO.seq(donors.schema.create)
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table DONORS created")
    }

    //biosample
    if (!tables.exists(_.name.name == BIOSAMPLE_TABLE_NAME)) {
      val queries = DBIO.seq(
      bioSamples.schema.create
    )
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table BIOSAMPLES created")
    }

    //replicate
    if (!tables.exists(_.name.name == REPLICATE_TABLE_NAME)) {
      val queries = DBIO.seq(
      replicates.schema.create
    )
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table REPLICATES created")
    }

    //experimentType
    if (!tables.exists(_.name.name == EXPERIMENTTYPE_TABLE_NAME)) {
      val queries = DBIO.seq(
        experimentsType.schema.create
      )
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table EXPERIMENTSTYPE created")
    }

    //project
    if (!tables.exists(_.name.name == PROJECT_TABLE_NAME)) {
      val queries = DBIO.seq(
      projects.schema.create
      )
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table PROJECTS created")
    }

    //container
    if (!tables.exists(_.name.name == CONTAINER_TABLE_NAME)) {
      val queries = DBIO.seq(
      containers.schema.create
      )
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table CONTAINERS created")
    }

    //case
    if (!tables.exists(_.name.name == CASE_TABLE_NAME)) {
      val queries = DBIO.seq(
      cases.schema.create
      )
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table CASES created")
    }

    //item
    if (!tables.exists(_.name.name == ITEM_TABLE_NAME)) {
      val queries = DBIO.seq(
      items.schema.create
      )
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table ITEMS created")
    }

    if (!tables.exists(_.name.name == REPLICATEITEM_TABLE_NAME)) {
      val queries = DBIO.seq(
        replicatesItems.schema.create
      )
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table REPLICATESITEMS created")
    }

    //caseitem
    if (!tables.exists(_.name.name == CASEITEM_TABLE_NAME)) {
      val queries = DBIO.seq(
      casesItems.schema.create
      )
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table CASESITEMS created")
    }

    if (!tables.exists(_.name.name == DERIVEDFROM_TABLE_NAME)) {
      val queries = DBIO.seq(
        derivedFrom.schema.create
      )
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table DERIVEDFROM created")
    }

    if (!tables.exists(_.name.name == CASE_TCGA_MAPPING)) {
      val queries = DBIO.seq(
        caseTcgaMapping.schema.create
      )
      /*for (line <- Source.fromFile((getClass.getResource("/mapping.csv").getFile)).getLines) {
        val cols = line.split(",").map(_.trim)
        insertCaseTcgaMapping(cols(0),cols(1))
      }*/
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table CASE TCGA MAPPING created")
    }
  }

  def closeDatabase(): Unit = {
    val closing = database.shutdown
    Await.result(closing,Duration.Inf)
  }

  //Insert Method

  def insertDonor(sourceId: String, species : String, age: Int, gender: String, ethnicity: String): Int ={
    val idQuery = (donors returning donors.map(_.donorId)) += (None, sourceId, Option(species), Option(age), Option(gender), Option(ethnicity), None, None)
    val executionId = database.run(idQuery)
    val id = Await.result(executionId, Duration.Inf)
    id
  }

  def updateDonor(sourceId: String, species : String, age: Int, gender: String, ethnicity: String): Int ={
    val query = for { donor <- donors if donor.sourceId === sourceId } yield (donor.species, donor.age, donor.gender, donor.ethnicity)
    val updateAction = query.update(Option(species),Option(age),Option(gender),Option(ethnicity))
    val execution = database.run(updateAction)
    Await.result(execution, Duration.Inf)
    val idQuery = donors.filter(_.sourceId === sourceId).map(_.donorId)
    val returnAction = idQuery.result
    val execution2 = database.run(returnAction)
    val id = Await.result(execution2,Duration.Inf)
    id.head
  }

  def updateDonorById(donorId: Int, sourceId: String, species : String, age: Int, gender: String, ethnicity: String): Int ={
    val query = for { donor <- donors if donor.donorId === donorId } yield (donor.sourceId, donor.species, donor.age, donor.gender, donor.ethnicity)
    val updateAction = query.update(sourceId, Option(species),Option(age),Option(gender),Option(ethnicity))
    val execution = database.run(updateAction)
    Await.result(execution, Duration.Inf)
    donorId
  }

  /*def insertOrUpdate(donorId: Int, sourceId: String, species : String, age: Int, gender: String, ethnicity: String): Int = {
    val query = donors.filter(_.sourceId === sourceId).map(_.donorId)
    val action = query.result
    val result = database.run(action)
    if(checkId(result) > 0) {
      val idQuery = (donors returning donors.map(_.donorId)) += (None, sourceId, Option(species), Option(age), Option(gender), Option(ethnicity))
      val executionId = database.run(idQuery)
      val newId = Await.result(executionId, Duration.Inf)
      return newId
    }
    else{
      val query = for { donor <- donors if donor.donorId === donorId } yield (donor.sourceId, donor.species, donor.age, donor.gender, donor.ethnicity)
      val updateAction = query.update(sourceId, Option(species),Option(age),Option(gender),Option(ethnicity))
      val execution = database.run(updateAction)
      Await.result(execution, Duration.Inf)
      donorId
    }
  }*/

  def insertBioSample(donorId: Int, sourceId: String, types : String, tissue: String, cellLine: String, isHealty: Boolean, disease: String): Int ={
    val idQuery = (bioSamples returning bioSamples.map(_.bioSampleId)) += (None, donorId, sourceId, Option(types), Option(tissue), Option(cellLine), isHealty, Option(disease), None, None)
    val executionId = database.run(idQuery)
    val id = Await.result(executionId, Duration.Inf)
    id
  }

  def updateBioSample(donorId: Int, sourceId: String, types : String, tIussue: String, cellLine: String, isHealthy: Boolean, disease: String): Int ={
    val query = for { bioSample <- bioSamples if bioSample.sourceId === sourceId } yield (bioSample.donorId,bioSample.types, bioSample.tissue, bioSample.cellLine, bioSample.isHealthy, bioSample.disease)
    val updateAction = query.update(donorId,Option(types),Option(tIussue),Option(cellLine),isHealthy,Option(disease))
    val execution = database.run(updateAction)
    Await.result(execution, Duration.Inf)
    val idQuery = bioSamples.filter(_.sourceId === sourceId).map(_.bioSampleId)
    val returnAction = idQuery.result
    val execution2 = database.run(returnAction)
    val id = Await.result(execution2,Duration.Inf)
    id.head
  }

  def updateBioSampleById(bioSampleId: Int, donorId: Int, sourceId: String, types : String, tIussue: String, cellLine: String, isHealthy: Boolean, disease: String): Int ={
    val query = for { bioSample <- bioSamples if bioSample.bioSampleId === bioSampleId} yield (bioSample.donorId, bioSample.sourceId,bioSample.types, bioSample.tissue, bioSample.cellLine, bioSample.isHealthy, bioSample.disease)
    val updateAction = query.update(donorId, sourceId, Option(types), Option(tIussue), Option(cellLine), isHealthy, Option(disease))
    val execution = database.run(updateAction)
    Await.result(execution, Duration.Inf)
    bioSampleId
  }

  def insertReplicate(bioSampleId: Int, sourceId: String, bioReplicateNum : Int, techReplicateNum: Int): Int ={
    val idQuery = (replicates returning replicates.map(_.replicateId))+= (None, bioSampleId, sourceId, Option(bioReplicateNum), Option(techReplicateNum))
    val executionId = database.run(idQuery)
    val id = Await.result(executionId, Duration.Inf)
    id
  }

  def updateReplicate(bioSampleId: Int, sourceId: String, bioReplicateNum : Int, techReplicateNum: Int): Int ={
    val query = for { replicate <- replicates if replicate.sourceId === sourceId } yield (replicate.bioSampleId, replicate.bioReplicateNum, replicate.techReplicateNum)
    val updateAction = query.update(bioSampleId,Option(bioReplicateNum),Option(techReplicateNum))
    val execution = database.run(updateAction)
    Await.result(execution, Duration.Inf)
    val idQuery = replicates.filter(_.sourceId === sourceId).map(_.replicateId)
    val returnAction = idQuery.result
    val execution2 = database.run(returnAction)
    val id = Await.result(execution2,Duration.Inf)
    id.head
  }

  def updateReplicateById(replicateId: Int, bioSampleId: Int, sourceId: String, bioReplicateNum : Int, techReplicateNum: Int): Int ={
    val query = for { replicate <- replicates if replicate.replicateId === replicateId } yield (replicate.bioSampleId, replicate.sourceId,replicate.bioReplicateNum, replicate.techReplicateNum)
    val updateAction = query.update(bioSampleId,sourceId, Option(bioReplicateNum),Option(techReplicateNum))
    val execution = database.run(updateAction)
    Await.result(execution, Duration.Inf)
    replicateId
  }

  def insertExperimentType(technique: String, feature: String, target: String, antibody: String): Int ={
    val idQuery = (experimentsType returning experimentsType.map(_.experimentTypeId))+= (None, Option(technique), Option(feature), Option(target), Option(antibody), None, None, None)
    val executionId = database.run(idQuery)
    val id = Await.result(executionId, Duration.Inf)
    id
  }

  def updateExperimentType(technique: String, feature: String, target: String, antibody: String): Int ={
    val query = for { experimentType <- experimentsType
                      if experimentType.technique === technique && experimentType.feature === feature && experimentType.target === target }
      yield experimentType.antibody
    val updateAction = query.update(Option(antibody))
    val execution = database.run(updateAction)
    Await.result(execution, Duration.Inf)
    val idQuery = experimentsType.filter(value => { value.technique === technique && value.feature === feature && value.target === target}).map(_.experimentTypeId)
    val returnAction = idQuery.result
    val execution2 = database.run(returnAction)
    val id = Await.result(execution2,Duration.Inf)
    id.head
  }

  def updateExperimentTypeById(experimentTypeId: Int, technique: String, feature: String, target: String, antibody: String): Int ={
    val query = for { experimentType <- experimentsType
                      if experimentType.experimentTypeId === experimentTypeId}
      yield (experimentType.technique, experimentType.feature, experimentType.target, experimentType.antibody)
    val updateAction = query.update(Option(technique), Option(feature), Option(target), Option(antibody))
    val execution = database.run(updateAction)
    Await.result(execution, Duration.Inf)
    experimentTypeId
  }

  def insertProject(projectName: String, programName: String): Int ={
    val idQuery = (projects returning projects.map(_.projectId)) += (None, projectName, Option(programName))
    val executionId = database.run(idQuery)
    val id = Await.result(executionId, Duration.Inf)
    id
  }

  def updateProject(projectName: String, programName: String): Int ={
    val query = for { project <- projects if project.projectName === projectName } yield project.programName
    val updateAction = query.update(Option(programName))
    val execution = database.run(updateAction)
    Await.result(execution, Duration.Inf)
    val idQuery = projects.filter(_.projectName === projectName).map(_.projectId)
    val returnAction = idQuery.result
    val execution2 = database.run(returnAction)
    val id = Await.result(execution2,Duration.Inf)
    id.head
  }

  def updateProjectById(projectId: Int, projectName: String, programName: String): Int ={
    val query = for { project <- projects if project.projectId === projectId } yield (project.projectName, project.programName)
    val updateAction = query.update(projectName, Option(programName))
    val execution = database.run(updateAction)
    Await.result(execution, Duration.Inf)
    projectId
  }

  def insertCase(containerId: Int, sourceId: String, sourceSite: String, externalRef: String): Int ={
    val idQuery = (cases returning cases.map(_.caseId))+= (None, containerId, sourceId, Option(sourceSite), Option(externalRef))
    val executionId = database.run(idQuery)
    val id = Await.result(executionId, Duration.Inf)
    id
  }

  def updateCase(containerId: Int, sourceId: String, sourceSite: String, externalRef: String): Int ={
    val query = for { cas <- cases if cas.sourceId === sourceId } yield (cas.containerId, cas.sourceSite, cas.externalRef)
    val updateAction = query.update(containerId, Option(sourceSite), Option(externalRef))
    val execution = database.run(updateAction)
    Await.result(execution, Duration.Inf)
    val idQuery = cases.filter(_.sourceId === sourceId).map(_.caseId)
    val returnAction = idQuery.result
    val execution2 = database.run(returnAction)
    val id = Await.result(execution2,Duration.Inf)
    id.head
  }

  def updateCaseById(caseId: Int, containerId: Int, sourceId: String, sourceSite: String, externalRef: String): Int ={
    val query = for { cas <- cases if cas.caseId === caseId } yield (cas.sourceId, cas.containerId, cas.sourceSite, cas.externalRef)
    val updateAction = query.update(sourceId, containerId, Option(sourceSite), Option(externalRef))
    val execution = database.run(updateAction)
    Await.result(execution, Duration.Inf)
    caseId
  }

  def insertContainer(projectId: Int, name: String, assembly: String, isAnn: Boolean, annotation: String): Int ={
    val idQuery = (containers returning containers.map(_.containerId))+= (None, projectId, name, Option(assembly), isAnn, Option(annotation), None)
    val executionId = database.run(idQuery)
    val id = Await.result(executionId, Duration.Inf)
    id
  }

  def updateContainer(projectId: Int, name: String, assembly: String, isAnn: Boolean, annotation: String): Int ={
    val query = for { container <- containers if container.name === name } yield (container.projectId, container.name, container.assembly, container.isAnn, container.annotation)
    val updateAction = query.update(projectId, name, Option(assembly), isAnn, Option(annotation))
    val execution = database.run(updateAction)
    Await.result(execution, Duration.Inf)
    val idQuery = containers.filter(_.name === name).map(_.containerId)
    val returnAction = idQuery.result
    val execution2 = database.run(returnAction)
    val id = Await.result(execution2,Duration.Inf)
    id.head
  }

  def updateContainerById(containerId: Int, projectId: Int, name: String, assembly: String, isAnn: Boolean, annotation: String): Int ={
    val query = for { container <- containers if container.containerId === containerId } yield (container.projectId, container.name, container.assembly, container.isAnn, container.annotation)
    val updateAction = query.update(projectId, name, Option(assembly), isAnn, Option(annotation))
    val execution = database.run(updateAction)
    Await.result(execution, Duration.Inf)
    containerId
  }

  def insertItem(experimentTypeId: Int, sourceId: String, dataType: String, format: String, size: Long, platform: String,  pipeline: String, sourceUrl: String, localUrl: String): Int ={
    val idQuery = (items returning items.map(_.itemId))+= (None, experimentTypeId, sourceId, Option(dataType), Option(format), Option(size), Option(platform), Option(pipeline), Option(sourceUrl), Option(localUrl), None)
    val executionId = database.run(idQuery)
    val id = Await.result(executionId, Duration.Inf)
    id
  }

  def updateItem(experimentTypeId: Int, sourceId: String, dataType: String, format: String, size: Long, platfrom: String, pipeline: String, sourceUrl: String, localUrl: String): Int ={
    val updateQuery = for { item <- items if item.sourceId === sourceId } yield (item.experimentTypeId, item.dataType, item.format, item.size, item.platform, item.pipeline, item.sourceUrl, item.localUrl)
    val updateAction = updateQuery.update(experimentTypeId, Option(dataType), Option(format), Option(size), Option(platfrom), Option(pipeline), Option(sourceUrl), Option(localUrl))
    val execution = database.run(updateAction)
    Await.result(execution, Duration.Inf)
    val idQuery = items.filter(_.sourceId === sourceId).map(_.itemId)
    val returnAction = idQuery.result
    val execution2 = database.run(returnAction)
    val id = Await.result(execution2,Duration.Inf)
    id.head
  }

  def updateItemById(itemId: Int, experimentTypeId: Int, sourceId: String, dataType: String, format: String, size: Long, platfrom: String, pipeline: String, sourceUrl: String, localUrl: String): Int ={
    val updateQuery = for { item <- items if item.itemId === itemId } yield (item.experimentTypeId, item.sourceId, item.dataType, item.format, item.size, item.platform, item.pipeline, item.sourceUrl, item.localUrl)
    val updateAction = updateQuery.update(experimentTypeId, sourceId, Option(dataType), Option(format), Option(size), Option(platfrom), Option(pipeline), Option(sourceUrl), Option(localUrl))
    val execution = database.run(updateAction)
    Await.result(execution, Duration.Inf)
    itemId
  }

  def insertReplicateItem(itemId: Int, replicateId: Int): Int ={
    val insertActions = DBIO.seq(
      replicatesItems += (itemId,replicateId)
    )
    Await.result(database.run(insertActions), Duration.Inf)
    1
  }

  def insertCaseItem(itemId: Int, caseId: Int): Int ={
    val insertActions = DBIO.seq(
      casesItems += (itemId,caseId)
    )
    Await.result(database.run(insertActions), Duration.Inf)
    1
  }

  def insertDerivedFrom(initialItemId: Int, finalItemId: Int, operation: String): Int ={
    val insertActions = DBIO.seq(
      derivedFrom += (initialItemId,finalItemId, Option(operation))
    )
    Await.result(database.run(insertActions), Duration.Inf)
    1
  }

  def updateDerivedFrom(initialItemId: Int, finalItemId: Int, operation: String): Int ={
    val query = for { derived <- derivedFrom if derived.initialItemId === initialItemId && derived.finalItemId === finalItemId } yield derived.operation
    val updateAction = query.update(Option(operation))
    val execution = database.run(updateAction)
    val id = Await.result(execution, Duration.Inf)
    id
  }

  def insertCaseTcgaMapping(code: String, sourceSite: String): Int = {
    val insertActions = DBIO.seq(
      caseTcgaMapping += (code, sourceSite)
    )
    Await.result(database.run(insertActions), Duration.Inf)
    1
  }

  /**
    *
    * @param result A general query
    * @return true if the element must be inserted,
    *         false if the element is already available in the Database
    */
  def checkResult(result: Future[Seq[Any]]): Boolean = {
    val res = Await.result(result, Duration.Inf)
    if(res.isEmpty)
      true
    else
      false
  }

  /**
    *
    * @param result A general query
    * @return the table id
    */
  def checkId(result: Future[Seq[Int]]): Int = {
    val res = Await.result(result, Duration.Inf)
    if(res.isEmpty)
      -1
    else
      res.head
  }

  def checkInsertDonor(sourceId: String): Boolean = {
    val query = donors.filter(_.sourceId === sourceId)
    val action = query.result
    val result = database.run(action)
    checkResult(result)
  }

  def checkInsertBioSample(sourceId: String): Boolean = {
    val query = bioSamples.filter(_.sourceId === sourceId)
    val action = query.result
    val result = database.run(action)
    checkResult(result)
  }

  def checkInsertReplicate(sourceId: String): Boolean = {
    val query = replicates.filter(_.sourceId === sourceId)
    val action = query.result
    val result = database.run(action)
    checkResult(result)
  }

  def checkInsertExperimentType(technique: String, feature: String, target: String): Boolean = {
    /*val query = experimentsType.filter(_.technique === technique)
    val action = query.result
    val result = database.run(action)
    checkResult(result)*/
    val query = experimentsType.filter( value => { value.technique === technique && value.feature === feature && value.target === target})
    val action = query.result
    val result = database.run(action)
    checkResult(result)
  }

  def checkInsertProject(projectName: String): Boolean = {
    val query = projects.filter(_.projectName === projectName)
    val action = query.result
    val result = database.run(action)
    checkResult(result)
  }

  def checkInsertCase(sourceId: String): Boolean = {
    val query = cases.filter(_.sourceId === sourceId)
    val action = query.result
    val result = database.run(action)
    checkResult(result)
  }

  def checkInsertContainer(name: String): Boolean = {
    val query = containers.filter(_.name === name)
    val action = query.result
    val result = database.run(action)
    checkResult(result)
  }

  def checkInsertItem(sourceId: String): Boolean = {
    val query = items.filter(_.sourceId === sourceId)
    val action = query.result
    val result = database.run(action)
    checkResult(result)
  }

  def checkInsertCaseItem(itemId: Int, caseId: Int): Boolean = {
    val query = casesItems.filter(_.itemId === itemId).filter(_.caseId === caseId)
    val action = query.result
    val result = database.run(action)
    checkResult(result)
  }

  def checkInsertReplicateItem(itemId: Int, replicateId: Int): Boolean = {
    val query = replicatesItems.filter(_.itemId === itemId).filter(_.replicateId === replicateId)
    val action = query.result
    val result = database.run(action)
    checkResult(result)
  }

  def checkInsertDerivedFrom(initialItemId: Int, finalItemId: Int): Boolean = {
    val query = derivedFrom.filter(_.initialItemId === initialItemId).filter(_.finalItemId === finalItemId)
    val action = query.result
    val result = database.run(action)
    checkResult(result)
  }


  def getDonorId(id: String): Int = {
    val query = donors.filter(_.sourceId === id).map(_.donorId)
    val action = query.result
    val result = database.run(action)
    checkId(result)
  }


  def getBioSampleId(sourceId: String): Int = {
    val query = bioSamples.filter(_.sourceId === sourceId).map(_.bioSampleId)
    val action = query.result
    val result = database.run(action)
    checkId(result)
  }

  def getReplicateId(sourceId: String): Int = {
    val query = replicates.filter(_.sourceId === sourceId).map(_.replicateId)
    val action = query.result
    val result = database.run(action)
    checkId(result)
  }

  def getExperimentTypeId(technique: String, feature: String, target: String): Int = {
    val query = experimentsType.filter(value => { value.technique === technique && value.feature === feature && value.target === target}).map(_.experimentTypeId)
    val action = query.result
    val result = database.run(action)
    checkId(result)
  }

  def getProjectId(projectName : String): Int = {
    val query = projects.filter(_.projectName === projectName).map(_.projectId)
    val action = query.result
    val result = database.run(action)
    checkId(result)
  }

  def getCaseId(sourceId : String): Int = {
    val query = cases.filter(_.sourceId === sourceId).map(_.caseId)
    val action = query.result
    val result = database.run(action)
    checkId(result)
  }

  def getContainerId(name : String): Int = {
    val query = containers.filter(_.name === name).map(_.containerId)
    val action = query.result
    val result = database.run(action)
    checkId(result)
  }

  def getItemId(sourceId : String): Int = {
    val query = items.filter(_.sourceId === sourceId).map(_.itemId)
    val action = query.result
    val result = database.run(action)
    checkId(result)
  }


  def getSourceSiteByCode(code : String): String ={
    val idQuery = caseTcgaMapping.filter(_.code === code).map(_.sourceSite)
    val returnAction = idQuery.result
    val execution2 = database.run(returnAction)
    val sourceSite = Await.result(execution2,Duration.Inf)
    sourceSite.head
  }

  /*def derivedFromId(initialItemId: Int, finalItemId: Int): Int = {
    val query = derivedFrom.filter(_.initialItemId === initialItemId).filter(_.finalItemId === finalItemId)
    val action = query.result
    val result = database.run(action)
    checkId(result)
  }*/

  /*def getCasesItemId(itemId : Int, caseId: Int): Int = {
    val query = casesItems.filter(_.itemId === itemId).filter(_.caseId === caseId).map(_.pk)
    val action = query.result
    val result = database.run(action)
    checkId(result)
  }*/

  def getDonorById(id: Int): Seq[(String, Option[String], Option[Int], Option[String], Option[String])] = {
    val query = for { donor <- donors if donor.donorId === id } yield (donor.sourceId, donor.species, donor.age, donor.gender, donor.ethnicity)
    val action = query.result
    val result = database.run(action)
    val res = Await.result(result, Duration.Inf)
    res
  }

  def getBiosampleById(id: Int): Seq[(Int, String, Option[String], Option[String], Option[String], Boolean, Option[String])] = {
    val query = for { bioSample <- bioSamples if bioSample.bioSampleId === id } yield (bioSample.donorId, bioSample.sourceId, bioSample.types, bioSample.tissue, bioSample.cellLine, bioSample.isHealthy, bioSample.disease)
    val action = query.result
    val result = database.run(action)
    val res = Await.result(result, Duration.Inf)
    res
  }

  def getItemBySourceId(sourceId: String): Seq[(Int, Int, String, Option[String], Option[String], Option[Long], Option[String], Option[String], Option[String], Option[String])] = {
    val query = for { item <- items if item.sourceId === sourceId } yield (item.itemId, item.experimentTypeId, item.sourceId, item.dataType, item.format, item.size, item.platform, item.pipeline, item.sourceUrl, item.localUrl)
    val action = query.result
    val result = database.run(action)
    val res = Await.result(result, Duration.Inf)
    res
  }


  def getContainerById(id: Int): Seq[(Int, Int, String, Option[String], Boolean, Option[String])] = {
    val query = for { container <- containers if container.containerId === id } yield (container.containerId, container.projectId, container.name, container.assembly, container.isAnn, container.annotation)
    val action = query.result
    val result = database.run(action)
    val res = Await.result(result, Duration.Inf)
    res
  }

  def getExperimentTypeById(id: Int): Seq[(Int, Option[String], Option[String], Option[String], Option[String])] = {
    val query = for { experimentType <- experimentsType if experimentType.experimentTypeId === id } yield (experimentType.experimentTypeId, experimentType.technique, experimentType.feature, experimentType.target, experimentType.antibody)
    val action = query.result
    val result = database.run(action)
    val res = Await.result(result, Duration.Inf)
    res
  }

  def getProjectById(id: Int): Seq[(String, Option[String])] = {
    val query = for { project <- projects if project.projectId === id } yield (project.projectName, project.programName)
    val action = query.result
    val result = database.run(action)
    val res = Await.result(result, Duration.Inf)
    res
  }

  def getCaseByItemId(itemId: Int): Seq[(Int, String, Option[String], Option[String])] ={
    val crossJoin = for {
      (caseItem, cases) <- casesItems.filter(_.itemId === itemId).join(cases).on(_.caseId === _.caseId)
    } yield (cases.containerId, cases.sourceId, cases.sourceSite, cases.externalRef)
    val action = crossJoin.result
    val result = database.run(action)
    val res = Await.result(result, Duration.Inf)
    res
  }

  def getReplicateByItemId (itemId: Int): Seq[(Int, String, Option[Int], Option[Int])] ={
    val crossJoin = for {
      (replicateItem, replicates) <- replicatesItems.filter(_.itemId === itemId).join(replicates).on(_.replicateId === _.replicateId)
    } yield (replicates.bioSampleId, replicates.sourceId, replicates.bioReplicateNum, replicates.techReplicateNum)
    val action = crossJoin.result
    val result = database.run(action)
    val res = Await.result(result, Duration.Inf)
    res
  }

  def getItemsByDerivedFromId (finalId: Int): Seq[(Int, Int, String, Option[String], Option[String], Option[Long], Option[String], Option[String], Option[String], Option[String])] ={
    val crossJoin = for {
      (derivedFrom, item) <- derivedFrom.filter(_.finalItemId === finalId).join(items).on(_.initialItemId === _.itemId)
    } yield (item.itemId, item.experimentTypeId, item.sourceId, item.dataType, item.format, item.size, item.platform, item.pipeline, item.sourceUrl, item.localUrl)
    val action = crossJoin.result
    val result = database.run(action)
    val res = Await.result(result, Duration.Inf)
    res
  }


  //-------------------------------------DATABASE SCHEMAS---------------------------------------------------------------

  //---------------------------------- Definition of the SOURCES table--------------------------------------------------

  class Donors(tag: Tag) extends
    Table[(Option[Int], String, Option[String], Option[Int], Option[String], Option[String],  Option[Int], Option[Int])](tag, DONOR_TABLE_NAME) {
    def donorId = column[Int]("donor_id", O.PrimaryKey, O.AutoInc)

    def sourceId = column[String]("source_id", O.Unique)

    def species = column[Option[String]]("species", O.Default(None))

    def age = column[Option[Int]]("age", O.Default(None))

    def gender = column[Option[String]]("gender", O.Default(None))

    def ethnicity = column[Option[String]]("ethnicity", O.Default(None))

    def speciesTid = column[Option[Int]]("species_tid", O.Default(None))

    def ethnicityTid = column[Option[Int]]("ethnicity_tid", O.Default(None))

    def * = (donorId.?, sourceId, species, age, gender, ethnicity, speciesTid, ethnicityTid)
  }

  val donors = TableQuery[Donors]

  class BioSamples(tag: Tag) extends
    Table[(Option[Int], Int, String, Option[String], Option[String], Option[String], Boolean, Option[String], Option[Int], Option[Int])](tag, BIOSAMPLE_TABLE_NAME) {
    def bioSampleId = column[Int]("biosample_id", O.PrimaryKey, O.AutoInc)

    def donorId = column[Int]("donor_id")

    def sourceId = column[String]("source_id", O.Unique)

    def types = column[Option[String]]("type", O.Default(None))

    def tissue = column[Option[String]]("tissue", O.Default(None))

    def cellLine = column[Option[String]]("cell_line", O.Default(None))

    def isHealthy = column[Boolean]("is_healthy")

    def disease = column[Option[String]]("disease", O.Default(None))

    def tissueTid = column[Option[Int]]("tissue_tid", O.Default(None))

    def cellLineTid = column[Option[Int]]("cell_line_tid", O.Default(None))

    def donor = foreignKey("biosamples_donor_fk", donorId, donors)(
      _.donorId,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

    def * = (bioSampleId.?, donorId, sourceId, types, tissue, cellLine, isHealthy, disease, tissueTid, cellLineTid)
  }


  val bioSamples = TableQuery[BioSamples]

  class Replicates(tag: Tag) extends
    Table[(Option[Int], Int, String, Option[Int], Option[Int])](tag, REPLICATE_TABLE_NAME) {
    def replicateId = column[Int]("replicate_id", O.PrimaryKey, O.AutoInc)

    def bioSampleId = column[Int]("biosample_id")

    def sourceId = column[String]("source_id", O.Unique)

    def bioReplicateNum = column[Option[Int]]("bio_replicate_num", O.Default(None))

    def techReplicateNum = column[Option[Int]]("tech_replicate_num", O.Default(None))

    def bioSample = foreignKey("replicates_donor_fk", bioSampleId, bioSamples)(
      _.bioSampleId,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

    def * = (replicateId.?, bioSampleId, sourceId, bioReplicateNum, techReplicateNum)
  }

  val replicates = TableQuery[Replicates]

  class ExperimentsType(tag: Tag) extends
    Table[(Option[Int], Option[String], Option[String], Option[String], Option[String], Option[Int], Option[Int], Option[Int])](tag, EXPERIMENTTYPE_TABLE_NAME) {
    def experimentTypeId = column[Int]("experiment_type_id", O.PrimaryKey, O.AutoInc)

    def technique = column[Option[String]]("technique", O.Default(None))

    def feature = column[Option[String]]("feature", O.Default(None))

    def target = column[Option[String]]("target", O.Default(None))

    def antibody = column[Option[String]]("antibody", O.Default(None))

    def techniqueTid = column[Option[Int]]("technique_tid", O.Default(None))

    def featureTid = column[Option[Int]]("feature_tid", O.Default(None))

    def targetTid = column[Option[Int]]("target_tid", O.Default(None))

    def uniqueKey = index("technique_feature_target", (technique, feature, target), unique = true)

    def * = (experimentTypeId.?, technique, feature, target, antibody, techniqueTid, featureTid, targetTid)
  }

  val experimentsType = TableQuery[ExperimentsType]

  class Projects(tag: Tag) extends
    Table[(Option[Int], String, Option[String])](tag, PROJECT_TABLE_NAME) {
    def projectId = column[Int]("project_id", O.PrimaryKey, O.AutoInc)

    def projectName =  column[String]("project_name", O.Unique)

    def programName =  column[Option[String]]("program_name", O.Default(None))

    def * = (projectId.?, projectName, programName)
  }

  val projects = TableQuery[Projects]

  class Cases(tag: Tag) extends
    Table[(Option[Int], Int, String, Option[String], Option[String])](tag, CASE_TABLE_NAME) {
    def caseId = column[Int]("case_study_id", O.PrimaryKey, O.AutoInc)

    def containerId = column[Int]("container_id")

    def sourceId = column[String]("source_id", O.Unique)

    def sourceSite = column[Option[String]]("source_site", O.Default(None))

    def externalRef = column[Option[String]]("external_ref", O.Default(None))

    def container = foreignKey("cases_container_fk", containerId, containers)(
      _.containerId,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

    def * = (caseId.?, containerId, sourceId, sourceSite, externalRef)
  }

  val cases = TableQuery[Cases]

  class Containers(tag: Tag) extends
    Table[(Option[Int], Int, String, Option[String], Boolean, Option[String], Option[Int])](tag, CONTAINER_TABLE_NAME) {
    def containerId = column[Int]("container_id", O.PrimaryKey, O.AutoInc)

    def projectId = column[Int]("project_id")

    def name = column[String]("name", O.Unique)

    def assembly = column[Option[String]]("assembly", O.Default(None))

    def isAnn = column[Boolean]("is_ann")

    def annotation = column[Option[String]]("annotation", O.Default(None))

    def annotationTid = column[Option[Int]]("annotation_tid", O.Default(None))

    def project = foreignKey("containers_project_fk", projectId, projects)(
      _.projectId,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

    def * = (containerId.?, projectId, name, assembly, isAnn, annotation, annotationTid)
  }

  val containers = TableQuery[Containers]

  class Items(tag: Tag) extends
    Table[(Option[Int], Int, String, Option[String], Option[String], Option[Long], Option[String], Option[String], Option[String], Option[String], Option[Int])](tag, ITEM_TABLE_NAME) {
    def itemId = column[Int]("item_id", O.PrimaryKey, O.AutoInc)

    def experimentTypeId = column[Int]("experiment_type_id")

    def sourceId = column[String]("source_id", O.Unique)

    def dataType = column[Option[String]]("data_type", O.Default(None))

    def format = column[Option[String]]("format", O.Default(None))

    def size = column[Option[Long]]("size", O.Default(None))

    def platform = column[Option[String]]("platform", O.Default(None))

    def pipeline = column[Option[String]]("pipeline", O.Default(None))

    def sourceUrl = column[Option[String]]("source_url", O.Default(None))

    def localUrl = column[Option[String]]("local_url", O.Default(None))

    def platformTid = column[Option[Int]]("platform_tid", O.Default(None))


    def experimentType = foreignKey("items_experimentType_fk", experimentTypeId, experimentsType)(
      _.experimentTypeId,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

    def * = (itemId.?, experimentTypeId, sourceId, dataType, format, size, platform, pipeline, sourceUrl, localUrl, platformTid)
  }

  val items = TableQuery[Items]

  class CasesItems(tag: Tag) extends
    Table[(Int, Int)](tag, CASEITEM_TABLE_NAME) {
    def itemId = column[Int]("item_id")

    def caseId = column[Int]("case_id")

    def pk = primaryKey("item_case_id", (itemId, caseId))

    def item = foreignKey("items_casesitem_fk", itemId, items)(
      _.itemId,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

    def caseFK = foreignKey("cases_caseitem_fk", caseId, cases)(
      _.caseId,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

    def * = (itemId, caseId)
  }

  val casesItems = TableQuery[CasesItems]

  class ReplicatesItems(tag: Tag) extends
    Table[(Int, Int)](tag, REPLICATEITEM_TABLE_NAME) {
    def itemId = column[Int]("item_id")

    def replicateId = column[Int]("replicate_id")

    def pk = primaryKey("item_replicate_id_replicatesitem", (itemId, replicateId))

    def item = foreignKey("items_replicateitem_fk", itemId, items)(
      _.itemId,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

    def caseFK = foreignKey("replicates_replicateitem_fk", replicateId, replicates)(
      _.replicateId,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

    def * = (itemId, replicateId)
  }

  val replicatesItems = TableQuery[ReplicatesItems]

  class DerivedFrom(tag: Tag) extends
    Table[(Int, Int, Option[String])](tag, DERIVEDFROM_TABLE_NAME) {

    def initialItemId = column[Int]("initial_item_id")

    def finalItemId = column[Int]("final_item_id")

    def operation = column[Option[String]]("operation", O.Default(None))

    def pk = primaryKey("item_replicate_id_derivedfrom", (initialItemId, finalItemId))

    def initialItemIdFK= foreignKey("items_initialitem_fk", initialItemId, items)(
      _.itemId,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

    def finalItemIdFK = foreignKey("items_finalitem_fk", finalItemId, items)(
      _.itemId,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

    def * = (initialItemId, finalItemId, operation)
  }

  val derivedFrom = TableQuery[DerivedFrom]

  class CaseTCGAMapping(tag: Tag) extends
    Table[(String, String)](tag, CASE_TCGA_MAPPING) {

    def code = column[String]("tss_code", O.PrimaryKey)

    def sourceSite = column[String]("source_site")

    def * = (code, sourceSite)
  }

  val caseTcgaMapping = TableQuery[CaseTCGAMapping]
}
