package it.polimi.genomics.importer.RemoteDatabase

import com.typesafe.config.ConfigFactory
import org.slf4j.{Logger, LoggerFactory}
//import slick.driver.PostgresDriver.api._
import slick.driver.MySQLDriver.api._

import slick.jdbc.meta.MTable
import slick.lifted.Tag

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}


object DbHandler {
  val conf = ConfigFactory.load()

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  //val connectionUrl = "jdbc:postgresql://131.175.120.18/geco-test?user=geco&password=geco78"

  //val driver = "com.mysql.jdbc.Driver"
 // val database = Database.forURL(connectionUrl, driver, keepAliveConnection = true)
 val database = Database.forURL(
   conf.getString("database.url"),
   conf.getString("database.username"),
   conf.getString("database.password"),
   driver=conf.getString("database.driver")
 )
  def setDatabase(): Unit = {

    val tables = Await.result(database.run(MTable.getTables), Duration.Inf).toList

    //for (table <- EncodeTableEnum.values) print(table +" ")
    //donors
    logger.info("Start to create the database")

    if (!tables.exists(_.name.name == "donors")) {
      var queries = DBIO.seq(donors.schema.create)
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table DONORS created")
    }

    //biosample
    if (!tables.exists(_.name.name == "biosamples")) {
      val queries = DBIO.seq(
      bioSamples.schema.create
    )
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table BIOSAMPLES created")
    }

    //replicate
    if (!tables.exists(_.name.name == "replicates")) {
      val queries = DBIO.seq(
      replicates.schema.create
    )
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table REPLICATES created")
    }

    //experimentType
    if (!tables.exists(_.name.name == "experimentstype")) {
      val queries = DBIO.seq(
        experimentsType.schema.create
      )
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table EXPERIMENTSTYPE created")
    }

    //project
    if (!tables.exists(_.name.name == "projects")) {
      val queries = DBIO.seq(
      projects.schema.create
      )
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table PROJECTS created")
    }

    //container
    if (!tables.exists(_.name.name == "containers")) {
      val queries = DBIO.seq(
      containers.schema.create
      )
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table CONTAINERS created")
    }

    //case
    if (!tables.exists(_.name.name == "cases")) {
      val queries = DBIO.seq(
      cases.schema.create
      )
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table CASES created")
    }

    //item
    if (!tables.exists(_.name.name == "items")) {
      val queries = DBIO.seq(
      items.schema.create
      )
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table ITEMS created")
    }

    if (!tables.exists(_.name.name == "replicatesitems")) {
      val queries = DBIO.seq(
        replicatesItems.schema.create
      )
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table REPLICATESITEMS created")
    }

    //caseitem
    if (!tables.exists(_.name.name == "casesitems")) {
      val queries = DBIO.seq(
      casesItems.schema.create
      )
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table CASESITEMS created")
    }

    if (!tables.exists(_.name.name == "derivedfrom")) {
      val queries = DBIO.seq(
        derivedFrom.schema.create
      )
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table DERIVEDFROM created")
    }
  }

  def closeDatabase(): Unit = {
    val closing = database.shutdown
    Await.result(closing,Duration.Inf)
  }

  //Insert Method

  def insertDonor(sourceId: String, species : String, age: Int, gender: String, ethnicity: String): Int ={
    val valori = (None, sourceId, Option(species), Option(age), Option(gender), Option(ethnicity))
    val idQuery = (donors returning donors.map(_.donorId)) += valori
    val executionId = database.run(idQuery)
    val id = Await.result(executionId, Duration.Inf)
    logger.info("Insert Donor: " + id + ", " + sourceId + ", " + species + ", " + age + ", " + gender + ", " + ethnicity)
    id
  }

  def updateDonor(sourceId: String, species : String, age: Int, gender: String, ethnicity: String): Int ={
    val query = for { donor <- donors if donor.sourceId === sourceId } yield (donor.species, donor.age, donor.gender, donor.ethnicity)
    val updateAction = query.update(Option(species),Option(age),Option(gender),Option(ethnicity))
    val execution = database.run(updateAction)
    val id = Await.result(execution, Duration.Inf)
    id
  }

  def insertBioSample(donorId: Int, sourceId: String, types : String, tIussue: String, cellLine: String, isHealty: Boolean, disease: String): Int ={
    val idQuery = (bioSamples returning bioSamples.map(_.bioSampleId)) += (None, donorId, sourceId, Option(types), Option(tIussue), Option(cellLine), isHealty, Option(disease))
    val executionId = database.run(idQuery)
    val id = Await.result(executionId, Duration.Inf)
    id
  }

  def updateBioSample(donorId: Int, sourceId: String, types : String, tIussue: String, cellLine: String, isHealty: Boolean, disease: String): Int ={
    val query = for { bioSample <- bioSamples if bioSample.sourceId === sourceId } yield (bioSample.donorId,bioSample.types, bioSample.tIssue, bioSample.cellLine, bioSample.isHealty, bioSample.disease)
    val updateAction = query.update(donorId,Option(types),Option(tIussue),Option(cellLine),isHealty,Option(disease))
    val execution = database.run(updateAction)
    val id = Await.result(execution, Duration.Inf)
    id
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
    val id = Await.result(execution, Duration.Inf)
    id
  }

  def insertExperimentType(technique: String, feature: String, target: String, antibody: String): Int ={
    val idQuery = (experimentsType returning experimentsType.map(_.experimentTypeId))+= (None, technique, Option(feature), Option(target), Option(antibody))
    val executionId = database.run(idQuery)
    val id = Await.result(executionId, Duration.Inf)
    id
  }

  def updateExperimentType(technique: String, feature: String, target: String, antibody: String): Int ={
    val query = for { experimentType <- experimentsType if experimentType.technique === technique } yield (experimentType.feature, experimentType.target, experimentType.antibody)
    val updateAction = query.update(Option(feature),Option(target),Option(antibody))
    val execution = database.run(updateAction)
    val id = Await.result(execution, Duration.Inf)
    id
  }

  def insertProject(projectName: String, programName: String): Int ={
    val idQuery = (projects returning projects.map(_.projectId)) += (None, projectName, Option(programName))
    val executionId = database.run(idQuery)
    val id = Await.result(executionId, Duration.Inf)
    id
  }

  def updateProject(projectName: String, programName: String): Int ={
    val query = for { project <- projects if project.projectName === programName } yield (project.programName)
    val updateAction = query.update(Option(programName))
    val execution = database.run(updateAction)
    val id = Await.result(execution, Duration.Inf)
    id
  }

  def insertCase(projectId: Int, sourceId: String, sourceSite: String, externalRef: String): Int ={
    val idQuery = (cases returning cases.map(_.caseId))+= (None, projectId, sourceId, Option(sourceSite), Option(externalRef))
    val executionId = database.run(idQuery)
    val id = Await.result(executionId, Duration.Inf)
    id
  }

  def updateCase(projectId: Int, sourceId: String, sourceSite: String, externalRef: String): Int ={
    val query = for { cas <- cases if cas.sourceId === sourceId } yield (cas.projectId, cas.sourceSite, cas.externalRef)
    val updateAction = query.update(projectId, Option(sourceSite), Option(externalRef))
    val execution = database.run(updateAction)
    val id = Await.result(execution, Duration.Inf)
    id
  }

  def insertContainer(experimentTypeId: Int, name: String, assembly: String, isAnn: Boolean, annotation: String): Int ={
    val idQuery = (containers returning containers.map(_.containerId))+= (None, experimentTypeId, Option(name), Option(assembly), isAnn, Option(annotation))
    val executionId = database.run(idQuery)
    val id = Await.result(executionId, Duration.Inf)
    id
  }

  def updateContainer(experimentTypeId: Int, name: String, assembly: String, isAnn: Boolean, annotation: String): Int ={
    val query = for { container <- containers if container.name === name } yield (container.experimentTypeId, container.assembly, container.isAnn, container.annotation)
    val updateAction = query.update(experimentTypeId,  Option(assembly), isAnn, Option(annotation))
    val execution = database.run(updateAction)
    val id = Await.result(execution, Duration.Inf)
    id
  }

  def insertItem(containerId: Int, sourceId: String, dataType: String, format: String, size: Long, platform: String,  pipeline: String, sourceUrl: String, localUrl: String): Int ={
    val idQuery = (items returning items.map(_.itemId))+= (None, containerId, sourceId, Option(dataType), Option(format), Option(size), Option(platform), Option(pipeline), Option(sourceUrl), Option(localUrl))
    val executionId = database.run(idQuery)
    val id = Await.result(executionId, Duration.Inf)
    id
  }

  def updateItem(containerId: Int, sourceId: String, dataType: String, format: String, size: Long, platfrom: String, pipeline: String, sourceUrl: String, localUrl: String): Int ={
    val updateQuery = for { item <- items if item.sourceId === sourceId } yield (item.containerId, item.dataType, item.format, item.size, item.platform, item.pipeline, item.sourceUrl, item.localUrl)
    val updateAction = updateQuery.update(containerId, Option(dataType), Option(format), Option(size), Option(platfrom), Option(pipeline), Option(sourceUrl), Option(localUrl))
    val execution = database.run(updateAction)
    Await.result(execution, Duration.Inf)
    val idQuery = items.filter(_.sourceId === sourceId).map(_.itemId)
    val returnAction = idQuery.result
    val execution2 = database.run(returnAction)
    val id = Await.result(execution2,Duration.Inf)
    id.head
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
    val query = for { derived <- derivedFrom if (derived.initialItemId === initialItemId && derived.finalItemId == finalItemId) } yield (derived.operation)
    val updateAction = query.update(Option(operation))
    val execution = database.run(updateAction)
    val id = Await.result(execution, Duration.Inf)
    id
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

  def checkInsertExperimentType(technique: String): Boolean = {
    val query = experimentsType.filter(_.technique === technique)
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

  def getExperimentTypeId(technique: String): Int = {
    val query = experimentsType.filter(_.technique === technique).map(_.experimentTypeId)
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


  //-------------------------------------DATABASE SCHEMAS---------------------------------------------------------------

  //---------------------------------- Definition of the SOURCES table--------------------------------------------------

  class Donors(tag: Tag) extends
    Table[(Option[Int], String, Option[String], Option[Int], Option[String], Option[String])](tag, "donors") {
    def donorId = column[Int]("DONOR_ID", O.PrimaryKey, O.AutoInc)

    def sourceId = column[String]("SOURCE_ID")

    def species = column[Option[String]]("SPECIES", O.Default(None))

    def age = column[Option[Int]]("AGE", O.Default(None))

    def gender = column[Option[String]]("GENDER", O.Default(None))

    def ethnicity = column[Option[String]]("ETHNICITY", O.Default(None))

    def * = (donorId.?, sourceId, species, age, gender, ethnicity)
  }

  val donors = TableQuery[Donors]

  class BioSamples(tag: Tag) extends
    Table[(Option[Int], Int, String, Option[String], Option[String], Option[String], Boolean, Option[String])](tag, "biosamples") {
    def bioSampleId = column[Int]("BIO_SAMPLE_ID", O.PrimaryKey, O.AutoInc)

    def donorId = column[Int]("DONOR_ID")

    def sourceId = column[String]("SOURCE_ID")

    def types = column[Option[String]]("TYPE", O.Default(None))

    def tIssue = column[Option[String]]("TISSUE", O.Default(None))

    def cellLine = column[Option[String]]("CELLLINE", O.Default(None))

    def isHealty = column[Boolean]("IS_HEALTY")

    def disease = column[Option[String]]("DISEASE", O.Default(None))

    def donor = foreignKey("BIOSAMPLES_DONOR_FK", donorId, donors)(
      _.donorId,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

    def * = (bioSampleId.?, donorId, sourceId, types, tIssue, cellLine, isHealty, disease)
  }


  val bioSamples = TableQuery[BioSamples]

  class Replicates(tag: Tag) extends
    Table[(Option[Int], Int, String, Option[Int], Option[Int])](tag, "replicates") {
    def replicateId = column[Int]("REPLICATE_ID", O.PrimaryKey, O.AutoInc)

    def bioSampleId = column[Int]("BIO_SAMPLE_ID")

    def sourceId = column[String]("SOURCE_ID")

    def bioReplicateNum = column[Option[Int]]("BIO_REPLICATE_NUM", O.Default(None))

    def techReplicateNum = column[Option[Int]]("TECH_REPLICATE_NUM", O.Default(None))

    def bioSample = foreignKey("REPLICATES_DONOR_FK", bioSampleId, bioSamples)(
      _.bioSampleId,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

    def * = (replicateId.?, bioSampleId, sourceId, bioReplicateNum, techReplicateNum)
  }

  val replicates = TableQuery[Replicates]

  class ExperimentsType(tag: Tag) extends
    Table[(Option[Int], String, Option[String], Option[String], Option[String])](tag, "experimentstype") {
    def experimentTypeId = column[Int]("EXPERIMENT_TYPE_ID", O.PrimaryKey, O.AutoInc)

    def technique = column[String]("TECHNIQUE")

    def feature = column[Option[String]]("FEATURE", O.Default(None))

    def target = column[Option[String]]("TARGET", O.Default(None))

    def antibody = column[Option[String]]("ANTIBODY", O.Default(None))

    def * = (experimentTypeId.?, technique, feature, target, antibody)
  }

  val experimentsType = TableQuery[ExperimentsType]

  class Projects(tag: Tag) extends
    Table[(Option[Int], String, Option[String])](tag, "projects") {
    def projectId = column[Int]("PROJECT_ID", O.PrimaryKey, O.AutoInc)

    def projectName =  column[String]("PROJECT_NAME")

    def programName =  column[Option[String]]("PROGRAM_NAME", O.Default(None))

    def * = (projectId.?, projectName, programName)
  }

  val projects = TableQuery[Projects]

  class Cases(tag: Tag) extends
    Table[(Option[Int], Int, String, Option[String], Option[String])](tag, "cases") {
    def caseId = column[Int]("CASE_ID", O.PrimaryKey, O.AutoInc)

    def projectId = column[Int]("PROJECT_ID")

    def sourceId = column[String]("SOURCE_ID")

    def sourceSite = column[Option[String]]("SOURCE_SITE", O.Default(None))

    def externalRef = column[Option[String]]("EXTERNAL_REF", O.Default(None))

    def project = foreignKey("CASES_PROJECT_FK", projectId, projects)(
      _.projectId,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

    def * = (caseId.?, projectId, sourceId, sourceSite, externalRef)
  }

  val cases = TableQuery[Cases]

  class Containers(tag: Tag) extends
    Table[(Option[Int], Int, Option[String], Option[String], Boolean, Option[String])](tag, "containers") {
    def containerId = column[Int]("CONTAINER_ID", O.PrimaryKey, O.AutoInc)

    def experimentTypeId = column[Int]("EXPERIMENT_TYPE_ID")

    def name = column[Option[String]]("NAME", O.Default(None))

    def assembly = column[Option[String]]("ASSEMBLY", O.Default(None))

    def isAnn = column[Boolean]("IS_ANN")

    def annotation = column[Option[String]]("ANNOTATION", O.Default(None))

    def experimentType = foreignKey("CONTAINERS_EXPERIMENT_TYPE_FK", containerId, experimentsType)(
      _.experimentTypeId,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

    def * = (containerId.?, experimentTypeId, name, assembly, isAnn, annotation)
  }

  val containers = TableQuery[Containers]

  class Items(tag: Tag) extends
    Table[(Option[Int], Int, String, Option[String], Option[String], Option[Long], Option[String], Option[String], Option[String], Option[String])](tag, "items") {
    def itemId = column[Int]("ITEM_ID", O.PrimaryKey, O.AutoInc)

    def containerId = column[Int]("CONTAINER_ID")

    def sourceId = column[String]("SOURCE_ID")

    def dataType = column[Option[String]]("DATA_TYPE", O.Default(None))

    def format = column[Option[String]]("FORMAT", O.Default(None))

    def size = column[Option[Long]]("SIZE", O.Default(None))

    def platform = column[Option[String]]("PLATFORM", O.Default(None))

    def pipeline = column[Option[String]]("PIPELINE", O.Default(None))

    def sourceUrl = column[Option[String]]("SOURCE_URL", O.Default(None))

    def localUrl = column[Option[String]]("LOCAL_URL", O.Default(None))


    def container = foreignKey("ITEMS_CONTAINER_FK", containerId, containers)(
      _.containerId,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

    def * = (itemId.?, containerId, sourceId, dataType, format, size, platform, pipeline, sourceUrl, localUrl)
  }

  val items = TableQuery[Items]

  class CasesItems(tag: Tag) extends
    Table[(Int, Int)](tag, "casesitems") {
    def itemId = column[Int]("ITEM_ID")

    def caseId = column[Int]("CASE_ID")

    def pk = primaryKey("ITEM_CASE_ID", (itemId, caseId))

    def item = foreignKey("ITEMS_CASEITEM_FK", itemId, items)(
      _.itemId,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

    def caseFK = foreignKey("CASES_CASEITEM_FK", caseId, cases)(
      _.caseId,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

    def * = (itemId, caseId)
  }

  val casesItems = TableQuery[CasesItems]

  class ReplicatesItems(tag: Tag) extends
    Table[(Int, Int)](tag, "replicatesitems") {
    def itemId = column[Int]("ITEM_ID")

    def replicateId = column[Int]("REPLICATE_ID")

    def pk = primaryKey("ITEM_REPLICATE_ID", (itemId, replicateId))

    def item = foreignKey("ITEMS_REPLICATEITEM_FK", itemId, items)(
      _.itemId,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

    def caseFK = foreignKey("REPLICATES_REPLICATEITEM_FK", replicateId, replicates)(
      _.replicateId,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

    def * = (itemId, replicateId)
  }

  val replicatesItems = TableQuery[ReplicatesItems]

  class DerivedFrom(tag: Tag) extends
    Table[(Int, Int, Option[String])](tag, "derivedfrom") {

    def initialItemId = column[Int]("INITIAL_ITEM_ID")

    def finalItemId = column[Int]("FINAL_ITEM_ID")

    def operation = column[Option[String]]("OPERATION", O.Default(None))

    def pk = primaryKey("ITEM_REPLICATE_ID", (initialItemId, finalItemId))

    def initialItemIdFK= foreignKey("ITEMS_INITIALITEM_FK", initialItemId, items)(
      _.itemId,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

    def finalItemIdFK = foreignKey("ITEMS_FINALITEM_FK", finalItemId, items)(
      _.itemId,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

    def * = (initialItemId, finalItemId, operation)
  }

  val derivedFrom = TableQuery[DerivedFrom]

}
