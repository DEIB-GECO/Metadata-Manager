package it.polimi.genomics.importer.RemoteDatabase

import org.slf4j.{Logger, LoggerFactory}
import slick.driver.PostgresDriver.api._
import slick.jdbc.meta.MTable
import slick.lifted.Tag

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}


object DbHandler {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val connectionUrl = "jdbc:postgresql://131.175.120.18/geco-test?user=geco&password=geco78"
  val driver = "org.postgresql.Driver"
  val database = Database.forURL(connectionUrl, driver, keepAliveConnection = true)

  def setDatabase(): Unit = {

    val tables = Await.result(database.run(MTable.getTables), Duration.Inf).toList

    //for (table <- EncodeTableEnum.values) print(table +" ")
    //donors
    if (!tables.exists(_.name.name == "DONORS")) {
      var queries = DBIO.seq(donors.schema.create)
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table DONORS created")
    }

    //biosample
    if (!tables.exists(_.name.name == "BIOSAMPLES")) {
      val queries = DBIO.seq(
      bioSamples.schema.create
    )
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table BIOSAMPLES created")
    }

    //replicate
    if (!tables.exists(_.name.name == "REPLICATES")) {
      val queries = DBIO.seq(
      replicates.schema.create
    )
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table REPLICATES created")
    }

    //experimentType
    if (!tables.exists(_.name.name == "EXPERIMENTSTYPE")) {
      val queries = DBIO.seq(
        experimentsType.schema.create
      )
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table EXPERIMENTSTYPE created")
    }

    //project
    if (!tables.exists(_.name.name == "PROJECTS")) {
      val queries = DBIO.seq(
      projects.schema.create
      )
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table PROJECTS created")
    }

    //container
    if (!tables.exists(_.name.name == "CONTAINERS")) {
      val queries = DBIO.seq(
      containers.schema.create
      )
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table CONTAINERS created")
    }

    //case
    if (!tables.exists(_.name.name == "CASES")) {
      val queries = DBIO.seq(
      cases.schema.create
      )
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table CASES created")
    }

    //item
    if (!tables.exists(_.name.name == "ITEMS")) {
      val queries = DBIO.seq(
      items.schema.create
      )
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table ITEMS created")
    }

    //caseitem
    if (!tables.exists(_.name.name == "CASESITEMS")) {
      val queries = DBIO.seq(
      casesItems.schema.create
      )
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table CASESITEMS created")
    }
  }

  //Insert Method

  def insertDonor(sourceId: String, species : String, age: Int, gender: String, ethnicity: String): Int ={
    val idQuery = (donors returning donors.map(_.donorId)) += (None, sourceId, species, age, gender, ethnicity)
    val executionId = database.run(idQuery)
    val id = Await.result(executionId, Duration.Inf)
    id
  }

  def insertBioSample(donorId: Int, sourceId: String, types : String, tIussue: String, cellLine: String, isHealty: Boolean, disease: String): Int ={
    val idQuery = (bioSamples returning bioSamples.map(_.bioSampleId)) += (None, donorId, sourceId, types, tIussue, cellLine, isHealty, disease)
    val executionId = database.run(idQuery)
    val id = Await.result(executionId, Duration.Inf)
    id
  }

  def insertReplicate(bioSampleId: Int, sourceId: String, bioReplicateNum : Int, techReplicateNum: Int): Int ={
    val idQuery = (replicates returning replicates.map(_.replicateId))+= (None, bioSampleId, sourceId, bioReplicateNum, techReplicateNum)
    val executionId = database.run(idQuery)
    val id = Await.result(executionId, Duration.Inf)
    id
  }

  def insertExperimentType(technique: String, feature: String, platform : String, target: String, antibody: String): Int ={
    val idQuery = (experimentsType returning experimentsType.map(_.experimentTypeId))+= (None, technique, feature, platform, target, antibody)
    val executionId = database.run(idQuery)
    val id = Await.result(executionId, Duration.Inf)
    id
  }

  def insertProject(projectName: String, programName: String): Int ={
    val idQuery = (projects returning projects.map(_.projectId))+= (None, projectName, programName)
    val executionId = database.run(idQuery)
    val id = Await.result(executionId, Duration.Inf)
    id
  }

  def insertCase(projectId: Int, sourceId: String, sourceSite: String): Int ={
    val idQuery = (cases returning cases.map(_.caseId))+= (None, projectId, sourceId, sourceSite)
    val executionId = database.run(idQuery)
    val id = Await.result(executionId, Duration.Inf)
    id
  }

  def insertContainer(experimentTypeId: Int, name: String, assembly: String, isAnn: Boolean, annotation: String): Int ={
    val idQuery = (containers returning containers.map(_.containerId))+= (None, experimentTypeId, name, assembly, isAnn, annotation)
    val executionId = database.run(idQuery)
    val id = Await.result(executionId, Duration.Inf)
    id
  }

  def insertItem(replicateId: Int, containerId: Int, sourceId: String, dataType: String, format: String, size: Int, pipeline: String, sourceUrl: String, localUrl: String): Int ={
    val idQuery = (items returning items.map(_.itemId))+= (None, replicateId, containerId, sourceId, dataType, format, size, pipeline, sourceUrl, localUrl)
    val executionId = database.run(idQuery)
    val id = Await.result(executionId, Duration.Inf)
    id
  }

  def insertCaseItem(itemId: Int, caseId: Int): Int ={
   /* val idQuery = DBIO.seq(casesItems.map(c => (c.itemId, c.caseId)) += (itemId, caseId))
    val executionId = database.run(idQuery)
    val id = Await.result(executionId, Duration.Inf)
    -1 //valore  null per far funzionare il trait nelle Table*/
    val idQuery = (casesItems returning casesItems.map(_.itemId))+= (itemId,caseId)
    val executionId = database.run(idQuery)
    val id = Await.result(executionId, Duration.Inf)
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

  def checkInsertExperimentType(technique: String, platform: String): Boolean = {
    val query = experimentsType.filter(_.technique === technique).filter(_.platform === platform)
    val action = query.result
    val result = database.run(action)
    checkResult(result)
  }

  def checkInsertProjectId(projectName: String): Boolean = {
    val query = projects.filter(_.projectName === projectName)
    val action = query.result
    val result = database.run(action)
    checkResult(result)
  }

  def checkInsertCaseId(sourceId: String): Boolean = {
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

  def getExperimentTypeId(technique: String, platform: String): Int = {
    val query = experimentsType.filter(_.technique === technique).filter(_.platform === platform).map(_.experimentTypeId)
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

  /*def getCasesItemId(itemId : Int, caseId: Int): Int = {
    val query = casesItems.filter(_.itemId === itemId).filter(_.caseId === caseId).map(_.pk)
    val action = query.result
    val result = database.run(action)
    checkId(result)
  }*/
  //-------------------------------------DATABASE SCHEMAS---------------------------------------------------------------

  //---------------------------------- Definition of the SOURCES table--------------------------------------------------

  class Donors(tag: Tag) extends
    Table[(Option[Int], String, String, Int, String, String)](tag, "DONORS") {
    def donorId = column[Int]("DONOR_ID", O.PrimaryKey, O.AutoInc)

    def sourceId = column[String]("SOURCE_ID")

    def species = column[String]("SPECIES")

    def age = column[Int]("AGE")

    def gender = column[String]("GENDER")

    def ethnicity = column[String]("ETHNICITY")

    def * = (donorId.?, sourceId, species, age, gender, ethnicity)
  }

  val donors = TableQuery[Donors]

  class BioSamples(tag: Tag) extends
    Table[(Option[Int], Int, String, String, String, String, Boolean, String)](tag, "BIOSAMPLES") {
    def bioSampleId = column[Int]("BIO_SAMPLE_ID", O.PrimaryKey, O.AutoInc)

    def donorId = column[Int]("DONOR_ID")

    def sourceId = column[String]("SOURCE_ID")

    def types = column[String]("TYPE")

    def tIssue = column[String]("TISSUE")

    def cellLine = column[String]("CELLLINE")

    def isHealty = column[Boolean]("IS_HEALTY")

    def disease = column[String]("DISEASE")

    def donor = foreignKey("BIOSAMPLES_DONOR_FK", donorId, donors)(
      _.donorId,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

    def * = (bioSampleId.?, donorId, sourceId, types, tIssue, cellLine, isHealty, disease)
  }


  val bioSamples = TableQuery[BioSamples]

  class Replicates(tag: Tag) extends
    Table[(Option[Int], Int, String, Int, Int)](tag, "REPLICATES") {
    def replicateId = column[Int]("REPLICATE_ID", O.PrimaryKey, O.AutoInc)

    def bioSampleId = column[Int]("BIO_SAMPLE_ID")

    def sourceId = column[String]("SOURCE_ID")

    def bioReplicateNum = column[Int]("BIO_REPLICATE_NUM")

    def techReplicateNum = column[Int]("TECH_REPLICATE_NUM")

    def bioSample = foreignKey("REPLICATES_DONOR_FK", bioSampleId, bioSamples)(
      _.bioSampleId,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

    def * = (replicateId.?, bioSampleId, sourceId, bioReplicateNum, techReplicateNum)
  }

  val replicates = TableQuery[Replicates]

  class ExperimentsType(tag: Tag) extends
    Table[(Option[Int], String, String, String, String, String)](tag, "EXPERIMENTSTYPE") {
    def experimentTypeId = column[Int]("EXPERIMENT_TYPE_ID", O.PrimaryKey, O.AutoInc)

    def technique = column[String]("TECHNIQUE")

    def feature = column[String]("FEATURE")

    def platform = column[String]("PLATFORM")

    def target = column[String]("TARGET")

    def antibody = column[String]("ANTIBODY")

    def * = (experimentTypeId.?, technique, feature, platform, target, antibody)
  }

  val experimentsType = TableQuery[ExperimentsType]

  class Projects(tag: Tag) extends
    Table[(Option[Int], String, String)](tag, "PROJECTS") {
    def projectId = column[Int]("PROJECT_ID", O.PrimaryKey, O.AutoInc)

    def projectName = column[String]("PROJECT_NAME")

    def programName = column[String]("PROGRAM_NAME")

    def * = (projectId.?, projectName, programName)
  }

  val projects = TableQuery[Projects]

  class Cases(tag: Tag) extends
    Table[(Option[Int], Int, String, String)](tag, "CASES") {
    def caseId = column[Int]("CASE_ID", O.PrimaryKey, O.AutoInc)

    def projectId = column[Int]("PROJECT_ID")

    def sourceId = column[String]("SOURCE_ID")

    def sourceSite = column[String]("SOURCE_SITE")

    def project = foreignKey("CASES_PROJECT_FK", projectId, projects)(
      _.projectId,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

    def * = (caseId.?, projectId, sourceId, sourceSite)
  }

  val cases = TableQuery[Cases]

  class Containers(tag: Tag) extends
    Table[(Option[Int], Int, String, String, Boolean, String)](tag, "CONTAINERS") {
    def containerId = column[Int]("CONTAINER_ID", O.PrimaryKey, O.AutoInc)

    def experimentTypeId = column[Int]("EXPERIMENT_TYPE_ID")

    def name = column[String]("NAME")

    def assembly = column[String]("ASSEMBLY")

    def isAnn = column[Boolean]("IS_ANN")

    def annotation = column[String]("ANNOTATION")

    def experimentType = foreignKey("CONTAINERS_EXPERIMENT_TYPE_FK", containerId, experimentsType)(
      _.experimentTypeId,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

    def * = (containerId.?, experimentTypeId, name, assembly, isAnn, annotation)
  }

  val containers = TableQuery[Containers]

  class Items(tag: Tag) extends
    Table[(Option[Int], Int, Int, String, String, String, Int, String, String, String)](tag, "ITEMS") {
    def itemId = column[Int]("ITEM_ID", O.PrimaryKey, O.AutoInc)

    def replicateId = column[Int]("REPLICATE_ID")

    def containerId = column[Int]("CONTAINER_ID")

    def sourceId = column[String]("SOURCE_ID")

    def dataType = column[String]("DATA_TYPE")

    def format = column[String]("FORMAT")

    def size = column[Int]("SIZE")

    def pipeline = column[String]("PIPELINE")

    def sourceUrl = column[String]("SOURCE_URL")

    def localUrl = column[String]("LOCAL_URL")

    def replicate = foreignKey("ITEMS_REPLICATE_FK", replicateId, replicates)(
      _.replicateId,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

    def container = foreignKey("ITEMS_CONTAINER_FK", replicateId, containers)(
      _.containerId,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

    def * = (itemId.?, replicateId, containerId, sourceId, dataType, format, size, pipeline, sourceUrl, localUrl)
  }

  val items = TableQuery[Items]

  class CasesItems(tag: Tag) extends
    Table[(Int, Int)](tag, "CASESITEMS") {
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

}
