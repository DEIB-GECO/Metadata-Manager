package it.polimi.genomics.importer.RemoteDatabase

import org.joda.time.DateTime
import org.slf4j.{Logger, LoggerFactory}
import slick.dbio.Effect.Write
import slick.driver.PostgresDriver.api._
import slick.lifted.{ProvenShape, Tag}
import slick.jdbc.meta.MTable


import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Table {

  def setDatabase(): Unit = {
    val connectionUrl = "jdbc:postgresql://131.175.120.18/geco-test?user=geco&password=geco78"
    val driver = "org.postgresql.Driver"
    val database = Database.forURL(connectionUrl, driver, keepAliveConnection = true)
    val tables = Await.result(database.run(MTable.getTables), Duration.Inf).toList

    //donors
    var queries = DBIO.seq(
      donors.schema.create
    )

    var setup = database.run(queries)
    Await.result(setup, Duration.Inf)

    //biosample
    queries = DBIO.seq(
      bioSamples.schema.create
    )
    setup = database.run(queries)
    Await.result(setup, Duration.Inf)

    //replicate
    queries = DBIO.seq(
      replicates.schema.create
    )
    setup = database.run(queries)
    Await.result(setup, Duration.Inf)

    //experimentType
    queries = DBIO.seq(
      experimentsType.schema.create
    )
    setup = database.run(queries)
    Await.result(setup, Duration.Inf)

    //project
    queries = DBIO.seq(
      projects.schema.create
    )
    setup = database.run(queries)
    Await.result(setup, Duration.Inf)

    //container
    queries = DBIO.seq(
      containers.schema.create
    )
    setup = database.run(queries)
    Await.result(setup, Duration.Inf)

    //case
    queries = DBIO.seq(
      cases.schema.create
    )
    setup = database.run(queries)
    Await.result(setup, Duration.Inf)

    //item
    queries = DBIO.seq(
      items.schema.create
    )
    setup = database.run(queries)
    Await.result(setup, Duration.Inf)

    //caseitem
    queries = DBIO.seq(
      casesItems.schema.create
    )
    setup = database.run(queries)
    Await.result(setup, Duration.Inf)
  }

  //-------------------------------------DATABASE SCHEMAS---------------------------------------------------------------

  //---------------------------------- Definition of the SOURCES table--------------------------------------------------
  /**
    *
    * @param tag
    */
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
    Table[(Option[Int], Int, String, String, String, Boolean, String)](tag, "BIOSAMPLES") {
    def bioSampleId = column[Int]("BIO_SAMPLE_ID", O.PrimaryKey, O.AutoInc)

    def donorId = column[Int]("DONOR_ID")

    def sourceId = column[String]("SOURCE_ID")

    def types = column[String]("TYPE")

    def cellLine = column[String]("CELLLINE")

    def isHealty = column[Boolean]("IS_HEALTY")

    def disease = column[String]("DISEASE")

    def donor = foreignKey("BIOSAMPLES_DONOR_FK", donorId, donors)(
      _.donorId,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

    def * = (bioSampleId.?, donorId, sourceId, types, cellLine, isHealty, disease)
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
    Table[(Option[Int], Option[Int])](tag, "CASESITEMS") {
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

    def * = (itemId.?, caseId.?)
  }

  val casesItems = TableQuery[CasesItems]

}
