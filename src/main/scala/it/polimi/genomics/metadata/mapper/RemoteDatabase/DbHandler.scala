package it.polimi.genomics.metadata.mapper.RemoteDatabase

import com.typesafe.config.ConfigFactory
import it.polimi.genomics.metadata.mapper.RemoteDatabase.DbHandler.cohorts
import it.polimi.genomics.metadata.step.utils.ParameterUtil
import org.slf4j.{Logger, LoggerFactory}
import slick.driver.PostgresDriver
import slick.driver.PostgresDriver.api._
import slick.jdbc.meta.MTable
import slick.lifted.Tag

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}


object DbHandler {

  val conf = ConfigFactory.load()

  private val DONOR_TABLE_NAME = "donor"
  private val BIOSAMPLE_TABLE_NAME = "biosample"
  private val REPLICATE_TABLE_NAME = "replicate"
  private val CASE_TABLE_NAME = "case_study"
  private val DATASET_TABLE_NAME = "dataset"
  private val PROJECT_TABLE_NAME = "project"
  private val EXPERIMENTTYPE_TABLE_NAME = "experiment_type"
  private val ITEM_TABLE_NAME = "item"
  private val DERIVEDFROM_TABLE_NAME = "derived_from"
  private val CASEITEM_TABLE_NAME = "case2item"
  private val REPLICATEITEM_TABLE_NAME = "replicate2item"
  private val PAIR_TABLE_NAME = "pair"
  private val FLATTEN_VIEW_NAME = "flatten"

  //for Gwas
  private val COHORT_TABLE_NAME = "cohort"
  private val ANCESTRY_TABLE_NAME = "ancestry"


  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  var database: PostgresDriver.backend.DatabaseDef = _

  //Launched using Configuration.xml
  if (ParameterUtil.dbConnectionUrl != "" && ParameterUtil.dbConnectionUrl != "" && ParameterUtil.dbConnectionUrl != "" && ParameterUtil.dbConnectionUrl != "")
    database = Database.forURL(
      ParameterUtil.dbConnectionUrl,
      ParameterUtil.dbConnectionUser,
      ParameterUtil.dbConnectionPw,
      driver = ParameterUtil.dbConnectionDriver)
  else //Launched using mapper config
    database = Try {
      Database.forURL(
        conf.getString("database.url"),
        conf.getString("database.username"),
        conf.getString("database.password"),
        driver = conf.getString("database.driver"))
    } match { //Launched using mapper config but missing values
      case Success(value) => value
      case Failure(f) => {
        logger.info("Mapper database config are missing.", f)
        throw new Exception("Mapper database config are missing.")
      }
    }


  logger.info("Set connection to database: " + ParameterUtil.dbConnectionUrl + ", user: " + ParameterUtil.dbConnectionUser)

  def setDatabase(): Unit = {

    val tables = Await.result(database.run(MTable.getTables), Duration.Inf).toList

    logger.info("Connecting to the database...")

    //donor
    if (!tables.exists(_.name.name == DONOR_TABLE_NAME)) {
      val queries = DBIO.seq(donors.schema.create)
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table " + DONOR_TABLE_NAME + " created")
    }

    //biosample
    if (!tables.exists(_.name.name == BIOSAMPLE_TABLE_NAME)) {
      val queries = DBIO.seq(bioSamples.schema.create)
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table " + BIOSAMPLE_TABLE_NAME + " created")
    }

    //replicate
    if (!tables.exists(_.name.name == REPLICATE_TABLE_NAME)) {
      val queries = DBIO.seq(replicates.schema.create)
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table " + REPLICATE_TABLE_NAME + " created")
    }

    //experimentType
    if (!tables.exists(_.name.name == EXPERIMENTTYPE_TABLE_NAME)) {
      val queries = DBIO.seq(experimentsType.schema.create)
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table " + EXPERIMENTTYPE_TABLE_NAME + " created")
    }

    //project
    if (!tables.exists(_.name.name == PROJECT_TABLE_NAME)) {
      val queries = DBIO.seq(projects.schema.create)
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table " + PROJECT_TABLE_NAME + " created")
    }

    //dataset
    if (!tables.exists(_.name.name == DATASET_TABLE_NAME)) {
      val queries = DBIO.seq(datasets.schema.create)
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table " + DATASET_TABLE_NAME + " created")
    }

    //case_study
    if (!tables.exists(_.name.name == CASE_TABLE_NAME)) {
      val queries = DBIO.seq(cases.schema.create)
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table " + CASE_TABLE_NAME + " created")
    }

    //item
    if (!tables.exists(_.name.name == ITEM_TABLE_NAME)) {
      val queries = DBIO.seq(items.schema.create)
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table " + ITEM_TABLE_NAME + " created")
    }

    //replicateitem
    if (!tables.exists(_.name.name == REPLICATEITEM_TABLE_NAME)) {
      val queries = DBIO.seq(replicatesItems.schema.create)
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table " + REPLICATEITEM_TABLE_NAME + " created")
    }

    //caseitem
    if (!tables.exists(_.name.name == CASEITEM_TABLE_NAME)) {
      val queries = DBIO.seq(casesItems.schema.create)
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table " + CASEITEM_TABLE_NAME + " created")
    }

    //derivedfrom
    /*if (!tables.exists(_.name.name == DERIVEDFROM_TABLE_NAME)) {
      val queries = DBIO.seq(derivedFrom.schema.create)
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table " + DERIVEDFROM_TABLE_NAME + " created")
    }*/

    //pair
    if (!tables.exists(_.name.name == PAIR_TABLE_NAME)) {
      val queries = DBIO.seq(pairs.schema.create)
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table " + PAIR_TABLE_NAME + " created")
    }

    //Gwas
    //Cohort
    if (!tables.exists(_.name.name == COHORT_TABLE_NAME)) {
      val queries = DBIO.seq(cohorts.schema.create)
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table " + COHORT_TABLE_NAME + " created")
    }

    //ancestry
    if (!tables.exists(_.name.name == ANCESTRY_TABLE_NAME)) {
      val queries = DBIO.seq(ancestries.schema.create)
      val setup = database.run(queries)
      Await.result(setup, Duration.Inf)
      logger.info("Table " + ANCESTRY_TABLE_NAME + " created")
    }


    val createGCMIndexesTry = Try {
      val createGCMIndexes =
        sqlu"""CREATE INDEX ON donor (donor_id);
                CREATE INDEX ON biosample (biosample_id);
                CREATE INDEX ON replicate (replicate_id);
                CREATE INDEX ON dataset (dataset_id);
                CREATE INDEX ON experiment_type (experiment_type_id);
                CREATE INDEX ON case_study (case_study_id);
                CREATE INDEX ON project (project_id);
                CREATE INDEX ON replicate2item (item_id);
                CREATE INDEX ON replicate2item (replicate_id);
                CREATE INDEX ON case2item (item_id);
                CREATE INDEX ON case2item (case_study_id);
                CREATE INDEX ON biosample (lower(biosample_type));
                CREATE INDEX ON biosample (lower(tissue));
                CREATE INDEX ON biosample (lower(cell));
                CREATE INDEX ON biosample (is_healthy);
                CREATE INDEX ON biosample (lower(disease));
                CREATE INDEX ON donor (lower(species));
                CREATE INDEX ON donor (age);
                CREATE INDEX ON donor (lower(gender));
                CREATE INDEX ON donor (lower(ethnicity));
                CREATE INDEX ON dataset (lower(dataset_name));
                CREATE INDEX ON dataset (lower(data_type));
                CREATE INDEX ON dataset (lower(file_format));
                CREATE INDEX ON dataset (lower(assembly));
                CREATE INDEX ON dataset (is_annotation);
                CREATE INDEX ON case_study (lower(source_site));
                CREATE INDEX ON case_study (lower(external_reference));
                CREATE INDEX ON project (lower(source));
                CREATE INDEX ON project (lower(project_name));
                CREATE INDEX ON experiment_type (lower(technique));
                CREATE INDEX ON experiment_type (lower(feature));
                CREATE INDEX ON experiment_type (lower(target));
                CREATE INDEX ON experiment_type (lower(antibody));"""
      val result = database.run(createGCMIndexes)
      Await.result(result, Duration.Inf)
    } match {
      case Success(value) => logger.info("Created all indexes on GCM tables")
      case Failure(f) => {
        logger.info("creation of indexes on GCM tables generated an error", f)
        throw new Exception("creation of indexes on GCM tables generated an error")
      }
    }




  }

  def closeDatabase(): Unit = {
    val closing = database.shutdown
    Await.result(closing, Duration.Inf)
  }

  def toOption[T](value: T): Option[T] = {
    if (!value.equals(0) && !value.equals(0L))
      Option(value)
    else
      None
  }


  //Insert Methods

  //GWAS
  def insertCohort(itemId: Int, traitName: String, caseNumber_initial: Int, controlNumber_initial: Int, individualNumber_initial: Int, triosNumber_initial: Int,
                   caseNumber_replicate: Int, controlNumber_replicate: Int, individualNumber_replicate: Int, triosNumber_replicate: Int, sourceId: String): Int = {
    val idQuery = (cohorts returning cohorts.map(_.cohortId)) += (None, itemId, traitName, None, caseNumber_initial, controlNumber_initial, individualNumber_initial, triosNumber_initial,
                                                                       caseNumber_replicate, controlNumber_replicate, individualNumber_replicate, triosNumber_replicate, sourceId)
    val executionId = database.run(idQuery)
    val id = Await.result(executionId, Duration.Inf)
    id
  }

  def updateCohort(itemId: Int, traitName: String, caseNumber_initial: Int, controlNumber_initial: Int, individualNumber_initial: Int, triosNumber_initial: Int,
                   caseNumber_replicate: Int, controlNumber_replicate: Int, individualNumber_replicate: Int, triosNumber_replicate: Int, sourceId: String): Int = {
        val query = for {
      cohort <- cohorts if cohort.sourceId === sourceId
    }
      yield (cohort.itemId, cohort.traitName, cohort.traitNameTid, cohort.caseNumber_initial, cohort.controlNumber_initial, cohort.individualNumber_initial, cohort.triosNumber_initial,
                                              cohort.caseNumber_replicate, cohort.controlNumber_replicate, cohort.individualNumber_replicate, cohort.triosNumber_replicate)
    val updateAction = query.update(itemId, traitName, None, caseNumber_initial, controlNumber_initial, individualNumber_initial, triosNumber_initial,
                                                       caseNumber_replicate, controlNumber_replicate, individualNumber_replicate, triosNumber_replicate)
    val execution = database.run(updateAction)
    Await.result(execution, Duration.Inf)
    val idQuery = cohorts.filter(_.sourceId === sourceId).map(_.cohortId)
    val returnAction = idQuery.result
    val execution2 = database.run(returnAction)
    val id = Await.result(execution2, Duration.Inf)
    id.head
  }

  def updateCohortById(cohortId: Int, itemId: Int, traitName: String, caseNumber_initial: Int, controlNumber_initial: Int, individualNumber_initial: Int, triosNumber_initial: Int,
                       caseNumber_replicate: Int, controlNumber_replicate: Int, individualNumber_replicate: Int, triosNumber_replicate: Int, sourceId: String): Int = {
    val query = for {
      cohort <- cohorts if cohort.cohortId === cohortId
    } yield (cohort.itemId, cohort.traitName, cohort.caseNumber_initial, cohort.controlNumber_initial, cohort.individualNumber_initial, cohort.triosNumber_initial,
      cohort.caseNumber_replicate, cohort.controlNumber_replicate, cohort.individualNumber_replicate, cohort.triosNumber_replicate, cohort.sourceId)
    val updateAction = query.update(itemId, traitName, caseNumber_initial, controlNumber_initial, individualNumber_initial, triosNumber_initial,
                                                       caseNumber_replicate, controlNumber_replicate, individualNumber_replicate, triosNumber_replicate, sourceId)
    val execution = database.run(updateAction)
    Await.result(execution, Duration.Inf)
    cohortId
  }

  def insertAncestry(cohortId: Int, broadAncestralCategory: String, countryOfOrigin: String, countryOfRecruitment: String, numberOfIndividuals: Int, sourceId: String): Int = {
    val idQuery = (ancestries returning ancestries.map(_.ancestryId)) += (None, cohortId, this.toOption[String](broadAncestralCategory), this.toOption[String](countryOfOrigin), this.toOption[String](countryOfRecruitment), numberOfIndividuals, sourceId)
    val executionId = database.run(idQuery)
    val id = Await.result(executionId, Duration.Inf)
    id
  }

  def updateAncestry(cohortId: Int, broadAncestralCategory: String, countryOfOrigin: String, countryOfRecruitment: String, numberOfIndividuals: Int, sourceId: String): Int = {
    val query = for {
      ancestry <- ancestries if (ancestry.cohortId === cohortId && ancestry.broadAncestralCategory === broadAncestralCategory && ancestry.countryOfRecruitment === countryOfRecruitment && ancestry.numberOfIndividuals === numberOfIndividuals && ancestry.sourceId === sourceId)
    }
      yield (ancestry.broadAncestralCategory, ancestry.countryOfOrigin, ancestry.countryOfRecruitment, ancestry.numberOfIndividuals, ancestry.sourceId)
    val updateAction = query.update(Option(broadAncestralCategory), Option(countryOfOrigin), Option(countryOfRecruitment), numberOfIndividuals, sourceId)
    val execution = database.run(updateAction)
    Await.result(execution, Duration.Inf)
    val idQuery = ancestries.filter(_.cohortId === cohortId).map(_.ancestryId)
    val returnAction = idQuery.result
    val execution2 = database.run(returnAction)
    val id = Await.result(execution2, Duration.Inf)
    id.head
  }

  def updateAncestryById(ancestryId: Int, cohortId: Int, broadAncestralCategory: String, countryOfOrigin: String, countryOfRecruitment: String, numberOfIndividuals: Int, sourceId: String): Int = {
    val query = for {
      ancestry <- ancestries if ancestry.ancestryId === ancestryId
    } yield (ancestry.cohortId, ancestry.broadAncestralCategory, ancestry.countryOfOrigin, ancestry.countryOfRecruitment, ancestry.numberOfIndividuals, ancestry.sourceId)
    val updateAction = query.update(cohortId, Option(broadAncestralCategory), Option(countryOfOrigin), Option(countryOfRecruitment), numberOfIndividuals, sourceId)
    val execution = database.run(updateAction)
    Await.result(execution, Duration.Inf)
    ancestryId
  }

  def insertDonor(sourceId: String, species: String, age: Option[Int], gender: String, ethnicity: String, altDonorSourceId: String): Int = {
    val idQuery = (donors returning donors.map(_.donorId)) += (None, sourceId, Option(species), None, age, Option(gender), Option(ethnicity), None, Option(altDonorSourceId))
    val executionId = database.run(idQuery)
    val id = Await.result(executionId, Duration.Inf)
    id
  }

  def updateDonor(sourceId: String, species: String, age: Option[Int], gender: String, ethnicity: String, altDonorSourceId: String): Int = {
    val query = for {
      donor <- donors if donor.sourceId === sourceId
    }
      yield (donor.species, donor.speciesTid, donor.age, donor.gender, donor.ethnicity, donor.ethnicityTid, donor.altDonorSourceId)
    val updateAction = query.update(Option(species), None, age, Option(gender), Option(ethnicity), None, Option(altDonorSourceId))
    val execution = database.run(updateAction)
    Await.result(execution, Duration.Inf)
    val idQuery = donors.filter(_.sourceId === sourceId).map(_.donorId)
    val returnAction = idQuery.result
    val execution2 = database.run(returnAction)
    val id = Await.result(execution2, Duration.Inf)
    id.head
  }

  def updateDonorById(donorId: Int, sourceId: String, species: String, age: Option[Int], gender: String, ethnicity: String, altDonorSourceId: String): Int = {
    val query = for {
      donor <- donors if donor.donorId === donorId
    } yield (donor.sourceId, donor.species, donor.age, donor.gender, donor.ethnicity, donor.altDonorSourceId)
    val updateAction = query.update(sourceId, Option(species), age, Option(gender), Option(ethnicity), Option(altDonorSourceId))
    val execution = database.run(updateAction)
    Await.result(execution, Duration.Inf)
    donorId
  }

  def insertBioSample(donorId: Int, sourceId: String, types: String, tissue: String, cell: String, isHealthy: Option[Boolean], disease: Option[String], altBiosampleSourceId: String): Int = {
    val idQuery = (bioSamples returning bioSamples.map(_.bioSampleId)) += (None, donorId, sourceId, Option(types), Option(tissue), None, Option(cell), None, isHealthy, disease, None, Option(altBiosampleSourceId))
    val executionId = database.run(idQuery)
    val id = Await.result(executionId, Duration.Inf)
    id
  }

  def updateBioSample(donorId: Int, sourceId: String, types: String, tissue: String, cell: String, isHealthy: Option[Boolean], disease: Option[String], altBiosampleSourceId: String): Int = {
    val query = for {
      bioSample <- bioSamples if bioSample.sourceId === sourceId
    }
      yield (bioSample.donorId, bioSample.types, bioSample.tissue, bioSample.tissueTid,
        bioSample.cell, bioSample.cellTid, bioSample.isHealthy, bioSample.disease, bioSample.diseaseTid, bioSample.altBiosampleSourceId)
    val updateAction = query.update(donorId, Option(types), Option(tissue), None, Option(cell), None, isHealthy, disease, None, Option(altBiosampleSourceId))
    val execution = database.run(updateAction)
    Await.result(execution, Duration.Inf)
    val idQuery = bioSamples.filter(_.sourceId === sourceId).map(_.bioSampleId)
    val returnAction = idQuery.result
    val execution2 = database.run(returnAction)
    val id = Await.result(execution2, Duration.Inf)
    id.head
  }

  def updateBioSampleById(bioSampleId: Int, donorId: Int, sourceId: String, types: String, tissue: String, cell: String, isHealthy: Option[Boolean], disease: Option[String], altBiosampleSourceId: String): Int = {
    val query = for {
      bioSample <- bioSamples if bioSample.bioSampleId === bioSampleId
    }
      yield (bioSample.donorId, bioSample.sourceId, bioSample.types, bioSample.tissue, bioSample.cell, bioSample.isHealthy, bioSample.disease, bioSample.altBiosampleSourceId)
    val updateAction = query.update(donorId, sourceId, Option(types), Option(tissue), Option(cell), isHealthy, disease, Option(altBiosampleSourceId))
    val execution = database.run(updateAction)
    Await.result(execution, Duration.Inf)
    bioSampleId
  }

  def insertReplicate(bioSampleId: Int, sourceId: String, bioReplicateNum: Int, techReplicateNum: Int): Int = {
    val idQuery = (replicates returning replicates.map(_.replicateId)) += (None, bioSampleId, sourceId, this.toOption[Int](bioReplicateNum), this.toOption[Int](techReplicateNum))
    val executionId = database.run(idQuery)
    val id = Await.result(executionId, Duration.Inf)
    id
  }

  def updateReplicate(bioSampleId: Int, sourceId: String, bioReplicateNum: Int, techReplicateNum: Int): Int = {
    val query = for {
      replicate <- replicates if replicate.sourceId === sourceId
    } yield (replicate.bioSampleId, replicate.bioReplicateNum, replicate.techReplicateNum)
    val updateAction = query.update(bioSampleId, this.toOption[Int](bioReplicateNum), this.toOption[Int](techReplicateNum))
    val execution = database.run(updateAction)
    Await.result(execution, Duration.Inf)
    val idQuery = replicates.filter(_.sourceId === sourceId).map(_.replicateId)
    val returnAction = idQuery.result
    val execution2 = database.run(returnAction)
    val id = Await.result(execution2, Duration.Inf)
    id.head
  }

  def updateReplicateById(replicateId: Int, bioSampleId: Int, sourceId: String, bioReplicateNum: Int, techReplicateNum: Int): Int = {
    val query = for {
      replicate <- replicates if replicate.replicateId === replicateId
    } yield (replicate.bioSampleId, replicate.sourceId, replicate.bioReplicateNum, replicate.techReplicateNum)
    val updateAction = query.update(bioSampleId, sourceId, this.toOption[Int](bioReplicateNum), this.toOption[Int](techReplicateNum))
    val execution = database.run(updateAction)
    Await.result(execution, Duration.Inf)
    replicateId
  }

  def insertExperimentType(technique: String, feature: String, target: String, antibody: String): Int = {
    val idQuery = (experimentsType returning experimentsType.map(_.experimentTypeId)) += (None, Option(technique), None, Option(feature), None, Option(target), None, Option(antibody))
    val executionId = database.run(idQuery)
    val id = Await.result(executionId, Duration.Inf)
    id
  }

  def updateExperimentType(technique: String, feature: String, target: String, antibody: String): Int = {
    val query = for {
      experimentType <- experimentsType
      if experimentType.technique === technique && experimentType.feature === feature && experimentType.target === target
    }
      yield (experimentType.antibody, experimentType.techniqueTid, experimentType.featureTid, experimentType.targetTid)
    val updateAction = query.update(Option(antibody), None, None, None)
    val execution = database.run(updateAction)
    Await.result(execution, Duration.Inf)
    val idQuery = experimentsType.filter(value => {
      value.technique === technique && value.feature === feature && value.target === target
    }).map(_.experimentTypeId)
    val returnAction = idQuery.result
    val execution2 = database.run(returnAction)
    val id = Await.result(execution2, Duration.Inf)
    id.head
  }

  def updateExperimentTypeById(experimentTypeId: Int, technique: String, feature: String, target: String, antibody: String): Int = {
    val query = for {
      experimentType <- experimentsType
      if experimentType.experimentTypeId === experimentTypeId
    }
      yield (experimentType.technique, experimentType.feature, experimentType.target, experimentType.antibody)
    val updateAction = query.update(Option(technique), Option(feature), Option(target), Option(antibody))
    val execution = database.run(updateAction)
    Await.result(execution, Duration.Inf)
    experimentTypeId
  }

  def insertProject(projectName: String, programName: String): Int = {
    val idQuery = (projects returning projects.map(_.projectId)) += (None, projectName, Option(programName))
    val executionId = database.run(idQuery)
    val id = Await.result(executionId, Duration.Inf)
    id
  }

  def updateProject(projectName: String, programName: String): Int = {
    val query = for {
      project <- projects if project.projectName === projectName
    } yield project.programName
    val updateAction = query.update(Option(programName))
    val execution = database.run(updateAction)
    Await.result(execution, Duration.Inf)
    val idQuery = projects.filter(_.projectName === projectName).map(_.projectId)
    val returnAction = idQuery.result
    val execution2 = database.run(returnAction)
    val id = Await.result(execution2, Duration.Inf)
    id.head
  }

  def updateProjectById(projectId: Int, projectName: String, programName: String): Int = {
    val query = for {
      project <- projects if project.projectId === projectId
    } yield (project.projectName, project.programName)
    val updateAction = query.update(projectName, Option(programName))
    val execution = database.run(updateAction)
    Await.result(execution, Duration.Inf)
    projectId
  }

  def insertCase(projectId: Int, sourceId: String, sourceSite: String, externalRef: String, altCaseSourceId: String): Int = {
    val idQuery = (cases returning cases.map(_.caseId)) += (None, projectId, sourceId, Option(sourceSite), Option(externalRef), Option(altCaseSourceId))
    val executionId = database.run(idQuery)
    val id = Await.result(executionId, Duration.Inf)
    id
  }

  def updateCase(projectId: Int, sourceId: String, sourceSite: String, externalRef: String, altCaseSourceId: String): Int = {
    val query = for {
      cas <- cases if cas.sourceId === sourceId
    } yield (cas.projectId, cas.sourceSite, cas.externalRef, cas.altCaseSourceId)
    val updateAction = query.update(projectId, Option(sourceSite), Option(externalRef), Option(altCaseSourceId))
    val execution = database.run(updateAction)
    Await.result(execution, Duration.Inf)
    val idQuery = cases.filter(_.sourceId === sourceId).map(_.caseId)
    val returnAction = idQuery.result
    val execution2 = database.run(returnAction)
    val id = Await.result(execution2, Duration.Inf)
    id.head
  }

  def updateCaseById(caseId: Int, projectId: Int, sourceId: String, sourceSite: String, externalRef: String, altCaseSourceId:String): Int = {
    val query = for {
      cas <- cases if cas.caseId === caseId
    } yield (cas.sourceId, cas.projectId, cas.sourceSite, cas.externalRef, cas.altCaseSourceId)
    val updateAction = query.update(sourceId, projectId, Option(sourceSite), Option(externalRef), Option(altCaseSourceId))
    val execution = database.run(updateAction)
    Await.result(execution, Duration.Inf)
    caseId
  }

  def insertDataset(name: String, dataType: String, format: String, assembly: String, isAnn: Boolean): Int = {
    val idQuery = (datasets returning datasets.map(_.datasetId)) += (None, name, Option(dataType), Option(format), Option(assembly), Option(isAnn))
    val executionId = database.run(idQuery)
    val id = Await.result(executionId, Duration.Inf)
    id
  }

  def updateDataset(name: String, dataType: String, format: String, assembly: String, isAnn: Boolean): Int = {
    val query = for {
      dataset <- datasets if dataset.name === name
    }
      yield (dataset.dataType, dataset.format, dataset.assembly, dataset.isAnn)
    val updateAction = query.update(Option(dataType), Option(format), Option(assembly), Option(isAnn))
    val execution = database.run(updateAction)
    Await.result(execution, Duration.Inf)
    val idQuery = datasets.filter(_.name === name).map(_.datasetId)
    val returnAction = idQuery.result
    val execution2 = database.run(returnAction)
    val id = Await.result(execution2, Duration.Inf)
    id.head
  }

  def updateDatasetById(datasetId: Int, name: String, dataType: String, format: String, assembly: String, isAnn: Boolean): Int = {
    val query = for {
      dataset <- datasets if dataset.datasetId === datasetId
    }
      yield (dataset.name, dataset.dataType, dataset.format, dataset.assembly, dataset.isAnn)
    val updateAction = query.update(name, Option(dataType), Option(format), Option(assembly), Option(isAnn))
    val execution = database.run(updateAction)
    Await.result(execution, Duration.Inf)
    datasetId
  }

  def insertItem(experimentTypeId: Int, datasetId: Int, sourceId: String, size: Long, date: String, checksum: String,
                 contentType: String, platform: String, pipeline: String, sourceUrl: String,
                 localUrl: String, fileName: String, sourcePage: String, altItemSourceId: String): Int = {
    val idQuery = (items returning items.map(_.itemId)) += (None, experimentTypeId, datasetId, sourceId,
      this.toOption[Long](size), Option(date), Option(checksum), Option(contentType), None, Option(platform), None,
      Option(pipeline), Option(sourceUrl), Option(localUrl), Option(fileName), Option(sourcePage), Option(altItemSourceId))
    val executionId = database.run(idQuery)
    val id = Await.result(executionId, Duration.Inf)
    id
  }

  def updateItem(experimentTypeId: Int, datasetId: Int, sourceId: String, size: Long, date: String, checksum: String,
                 contentType: String, platform: String, pipeline: String, sourceUrl: String,
                 localUrl: String, fileName: String, sourcePage: String, altItemSourceId: String): Int = {
    val updateQuery = for {
      item <- items if item.sourceId === sourceId
    }
      yield (item.experimentTypeId, item.datasetId, item.size, item.date, item.checksum, item.contentType,
        item.platform, item.platformTid, item.pipeline, item.sourceUrl, item.localUrl, item.fileName, item.sourcePage, item.altItemSourceId)

    val updateAction = updateQuery.update(experimentTypeId, datasetId, this.toOption[Long](size), Option(date),
      Option(checksum), Option(contentType), Option(platform), None, Option(pipeline), Option(sourceUrl),
      Option(localUrl), Option(fileName), Option(sourcePage), Option(altItemSourceId))
    val execution = database.run(updateAction)
    Await.result(execution, Duration.Inf)
    val idQuery = items.filter(_.sourceId === sourceId).map(_.itemId)
    val returnAction = idQuery.result
    val execution2 = database.run(returnAction)
    val id = Await.result(execution2, Duration.Inf)
    id.head
  }

  def updateItemById(itemId: Int, experimentTypeId: Int, datasetId: Int, sourceId: String, size: Long, date: String,
                     checksum: String, contentType: String, platform: String, pipeline: String, sourceUrl: String,
                     localUrl: String, fileName: String, sourcePage: String, altItemSourceId: String): Int = {
    val updateQuery = for {
      item <- items if item.itemId === itemId
    } yield (item.experimentTypeId, item.datasetId,
      item.sourceId, item.size, item.date, item.checksum, item.contentType, item.platform, item.pipeline, item.sourceUrl,
      item.localUrl, item.fileName, item.sourcePage)
    val updateAction = updateQuery.update(experimentTypeId, datasetId, sourceId, this.toOption[Long](size), Option(date),
      Option(checksum), Option(contentType), Option(platform), Option(pipeline), Option(sourceUrl),
      Option(localUrl), Option(fileName), Option(sourcePage))
    val execution = database.run(updateAction)
    Await.result(execution, Duration.Inf)
    itemId
  }

  def insertReplicateItem(itemId: Int, replicateId: Int): Int = {
    val insertActions = DBIO.seq(
      replicatesItems += (itemId, replicateId)
    )
    Await.result(database.run(insertActions), Duration.Inf)
    1
  }

  def insertCaseItem(itemId: Int, caseId: Int): Int = {
    val insertActions = DBIO.seq(
      casesItems += (itemId, caseId)
    )
    Await.result(database.run(insertActions), Duration.Inf)
    1
  }

  /*def insertDerivedFrom(initialItemId: Int, finalItemId: Int, operation: String): Int = {
    val insertActions = DBIO.seq(
      derivedFrom += (initialItemId, finalItemId, Option(operation))
    )
    Await.result(database.run(insertActions), Duration.Inf)
    1
  }

  def updateDerivedFrom(initialItemId: Int, finalItemId: Int, operation: String): Int = {
    val query = for {
      derived <- derivedFrom if derived.initialItemId === initialItemId && derived.finalItemId === finalItemId
    } yield derived.operation
    val updateAction = query.update(Option(operation))
    val execution = database.run(updateAction)
    val id = Await.result(execution, Duration.Inf)
    id
  }
  */

  /**
    * Returns all the key value pairs of the given itemId
    *
    * @param itemId
    * @return
    */
  def getPairs(itemId: Int): Seq[(String, String)] = {
    val query = pairs.filter(p => p.itemId === itemId).map(t => (t.key, t.value)).result
    val execution = database.run(query)
    Await.result(execution, Duration.Inf)
  }

  //single insertions
  def insertPair(itemId: Int, key: String, value: String): Int = {
    val insertActions = DBIO.seq(
      pairs += (itemId, key, value)
    )
    Await.result(database.run(insertActions), Duration.Inf)
    1
  }

  //insertion of pairs with batch execution
  def insertPairBatch(itemId: Int, insertPairs: List[(String, String)]): Int = {
    val toBeInserted = insertPairs.map(p => pairs += (itemId, p._1, p._2))
    val inOneGo = DBIO.sequence(toBeInserted)
    val dbioFuture = database.run(inOneGo)
    Await.result(dbioFuture, Duration.Inf).sum
  }

  //deletion of pairs with batch execution
  def deletePairBatch(itemId: Int, deletePairs: List[(String, String)]): Int = {
    val toBeDeleted = deletePairs.map(del => pairs.filter(p => p.itemId === itemId && p.key === del._1 && p.value === del._2).delete)
    val inOneGo = DBIO.sequence(toBeDeleted)
    val dbioFuture = database.run(inOneGo)
    Await.result(dbioFuture, Duration.Inf).sum
  }

  /**
    *
    * @param result A general query
    * @return true if the element must be inserted,
    *         false if the element is already available in the Database
    */
  def checkResult(result: Future[Seq[Any]]): Boolean = {
    val res = Await.result(result, Duration.Inf)
    if (res.isEmpty)
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
    if (res.isEmpty)
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
    val query = experimentsType.filter(value => {
      value.technique === technique && value.feature === feature && value.target === target
    })
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

  def checkInsertDataset(name: String): Boolean = {
    val query = datasets.filter(_.name === name)
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

  /*
  def checkInsertDerivedFrom(initialItemId: Int, finalItemId: Int): Boolean = {
    val query = derivedFrom.filter(_.initialItemId === initialItemId).filter(_.finalItemId === finalItemId)
    val action = query.result
    val result = database.run(action)
    checkResult(result)
  }
  */

  def checkInsertPair(itemId: Int, key: String, value: String): Boolean = {
    val query = pairs.filter(_.itemId === itemId).filter(_.key === key).filter(_.value === value)
    val action = query.result
    val result = database.run(action)
    checkResult(result)
  }

  def checkInsertCohort(sourceId: String): Boolean = {
    val query = cohorts.filter(_.sourceId === sourceId)
    val action = query.result
    val result = database.run(action)
    checkResult(result)
  }

  def checkInsertAncestry(cohortId: Int, broadAncestralCategory: String, countryOfOrigin: String, countryOfRecruitment: String, numberOfIndividuals: Int): Boolean = {
    val query = ancestries.filter(value => {
      value.cohortId === cohortId && value.broadAncestralCategory === broadAncestralCategory && value.countryOfOrigin === countryOfOrigin &&
        value.countryOfRecruitment === countryOfRecruitment && value.numberOfIndividuals === numberOfIndividuals
    })
    val action = query.result
    val result = database.run(action)
    checkResult(result)
  }

  def getCohortId(id: String): Int = {
    val query = cohorts.filter(_.sourceId === id).map(_.cohortId)
    val action = query.result
    val result = database.run(action)
    checkId(result)
  }

  def getAncestryId(broadAncestralCategory: String, countryOfOrigin: String, countryOfRecruitment: String, numberOfIndividuals: Int, sourceId: String): Int = {
    val query = ancestries.filter(value => {
      value.broadAncestralCategory === broadAncestralCategory && value.countryOfOrigin === countryOfOrigin && value.countryOfRecruitment === countryOfRecruitment && value.numberOfIndividuals === numberOfIndividuals && value.sourceId === sourceId
    }).map(_.ancestryId)
    val action = query.result
    val result = database.run(action)
    checkId(result)
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
    val query = experimentsType.filter(value => {
      value.technique === technique && value.feature === feature && value.target === target
    }).map(_.experimentTypeId)
    val action = query.result
    val result = database.run(action)
    checkId(result)
  }

  def getProjectId(projectName: String): Int = {
    val query = projects.filter(_.projectName === projectName).map(_.projectId)
    val action = query.result
    val result = database.run(action)
    checkId(result)
  }

  def getCaseId(sourceId: String): Int = {
    val query = cases.filter(_.sourceId === sourceId).map(_.caseId)
    val action = query.result
    val result = database.run(action)
    checkId(result)
  }

  def getDatasetId(name: String): Int = {
    val query = datasets.filter(_.name === name).map(_.datasetId)
    val action = query.result
    val result = database.run(action)
    checkId(result)
  }

  def getItemId(sourceId: String): Int = {
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

  def getCohortById(id: Int): Seq[(Int, String, Int, Int, Int, Int, Int, Int, Int, Int, String)] = {
    val query = for {
      cohort <- cohorts if cohort.cohortId === id
    } yield (cohort.itemId, cohort.traitName, cohort.caseNumber_initial, cohort.controlNumber_initial, cohort.individualNumber_initial, cohort.triosNumber_initial,
      cohort.caseNumber_replicate, cohort.controlNumber_replicate, cohort.individualNumber_replicate, cohort.triosNumber_replicate, cohort.sourceId)
    val action = query.result
    val result = database.run(action)
    val res = Await.result(result, Duration.Inf)
    res
  }

  def getAncestryById(id: Int): Seq[(Int, Option[String], Option[String], Option[String], Int)] = {
    val query = for {
      ancestry <- ancestries if ancestry.ancestryId === id
    } yield (ancestry.cohortId, ancestry.broadAncestralCategory, ancestry.countryOfOrigin, ancestry.countryOfRecruitment, ancestry.numberOfIndividuals)
    val action = query.result
    val result = database.run(action)
    val res = Await.result(result, Duration.Inf)
    res
  }


  def getDonorById(id: Int): Seq[(String, Option[String], Option[Int], Option[String], Option[String], Option[String])] = {
    val query = for {
      donor <- donors if donor.donorId === id
    } yield (donor.sourceId, donor.species, donor.age, donor.gender, donor.ethnicity, donor.altDonorSourceId)
    val action = query.result
    val result = database.run(action)
    val res = Await.result(result, Duration.Inf)
    res
  }

  def getBiosampleById(id: Int): Seq[(Int, String, Option[String], Option[String], Option[String], Option[Boolean], Option[String])] = {
    val query = for {
      bioSample <- bioSamples if bioSample.bioSampleId === id
    } yield (bioSample.donorId, bioSample.sourceId, bioSample.types, bioSample.tissue, bioSample.cell, bioSample.isHealthy, bioSample.disease)
    val action = query.result
    val result = database.run(action)
    val res = Await.result(result, Duration.Inf)
    res
  }

  def getItemBySourceId(sourceId: String): Seq[(Int, Int, Int, String, Option[Long], Option[String], Option[String], Option[String], Option[String])] = {
    val query = for {
      item <- items if item.sourceId === sourceId
    } yield (item.itemId, item.experimentTypeId, item.datasetId, item.sourceId, item.size, item.pipeline, item.platform, item.sourceUrl, item.localUrl)
    val action = query.result
    val result = database.run(action)
    val res = Await.result(result, Duration.Inf)
    res
  }

  def getDatasetById(id: Int): Seq[(Int, String, Option[String], Option[String], Option[String], Option[Boolean])] = {
    val query = for {
      dataset <- datasets if dataset.datasetId === id
    } yield (dataset.datasetId, dataset.name, dataset.dataType, dataset.format, dataset.assembly, dataset.isAnn)
    val action = query.result
    val result = database.run(action)
    val res = Await.result(result, Duration.Inf)
    res
  }

  def getExperimentTypeById(id: Int): Seq[(Int, Option[String], Option[String], Option[String], Option[String])] = {
    val query = for {
      experimentType <- experimentsType if experimentType.experimentTypeId === id
    } yield (experimentType.experimentTypeId, experimentType.technique, experimentType.feature, experimentType.target, experimentType.antibody)
    val action = query.result
    val result = database.run(action)
    val res = Await.result(result, Duration.Inf)
    res
  }

  def getProjectById(id: Int): Seq[(String, Option[String])] = {
    val query = for {
      project <- projects if project.projectId === id
    } yield (project.projectName, project.programName)
    val action = query.result
    val result = database.run(action)
    val res = Await.result(result, Duration.Inf)
    res
  }

  def getCaseByItemId(itemId: Int): Seq[(Int, String, Option[String], Option[String])] = {
    val crossJoin = for {
      (caseItem, cases) <- casesItems.filter(_.itemId === itemId).join(cases).on(_.caseId === _.caseId)
    } yield (cases.projectId, cases.sourceId, cases.sourceSite, cases.externalRef)
    val action = crossJoin.result
    val result = database.run(action)
    val res = Await.result(result, Duration.Inf)
    res
  }

  def getReplicateByItemId(itemId: Int): Seq[(Int, String, Option[Int], Option[Int])] = {
    val crossJoin = for {
      (replicateItem, replicates) <- replicatesItems.filter(_.itemId === itemId).join(replicates).on(_.replicateId === _.replicateId)
    } yield (replicates.bioSampleId, replicates.sourceId, replicates.bioReplicateNum, replicates.techReplicateNum)
    val action = crossJoin.result
    val result = database.run(action)
    val res = Await.result(result, Duration.Inf)
    res
  }

  /*
  def getItemsByDerivedFromId(finalId: Int): Seq[(Int, Int, Int, String, Option[Long], Option[String], Option[String], Option[String], Option[String])] = {
    val crossJoin = for {
      (derivedFrom, item) <- derivedFrom.filter(_.finalItemId === finalId).join(items).on(_.initialItemId === _.itemId)
    } yield (item.itemId, item.experimentTypeId, item.datasetId, item.sourceId, item.size, item.platform, item.pipeline, item.sourceUrl, item.localUrl)
    val action = crossJoin.result
    val result = database.run(action)
    val res = Await.result(result, Duration.Inf)
    res
  }
  */


  //-------------------------------------DATABASE SCHEMAS---------------------------------------------------------------

  //---------------------------------- Definition of the SOURCES table--------------------------------------------------

  class Cohort(tag: Tag) extends
    Table[(Option[Int], Int, String, Option[Int], Int, Int, Int, Int, Int, Int, Int, Int, String)](tag, COHORT_TABLE_NAME) {
    def cohortId = column[Int]("cohort_id", O.PrimaryKey, O.AutoInc)

    def itemId = column[Int]("item_id")

    def traitName = column[String]("trait_name")

    def traitNameTid = column[Option[Int]]("trait_name_tid", O.Default(None))

    def caseNumber_initial = column[Int]("case_number_initial")

    def controlNumber_initial = column[Int]("control_number_initial")

    def individualNumber_initial = column[Int]("individual_number_initial")

    def triosNumber_initial = column[Int]("trios_number_initial")

    def caseNumber_replicate = column[Int]("case_number_replicate")

    def controlNumber_replicate = column[Int]("control_number_replicate")

    def individualNumber_replicate = column[Int]("individual_number_replicate")

    def triosNumber_replicate = column[Int]("trios_number_replicate")

    def sourceId = column[String]("cohort_source_id", O.Unique)

    def item = foreignKey("cohort_item_fk", itemId, items)(
      _.itemId,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

    def * = (cohortId.?, itemId, traitName, traitNameTid, caseNumber_initial, controlNumber_initial, individualNumber_initial, triosNumber_initial,
                    caseNumber_replicate, controlNumber_replicate, individualNumber_replicate, triosNumber_replicate, sourceId)
  }

  val cohorts = TableQuery[Cohort]

  class Ancestry(tag: Tag) extends
    Table[(Option[Int], Int, Option[String], Option[String], Option[String], Int, String)](tag, ANCESTRY_TABLE_NAME) {
    def ancestryId = column[Int]("ancestry_id", O.PrimaryKey, O.AutoInc)

    def cohortId = column[Int]("cohort_id")

    def broadAncestralCategory = column[Option[String]]("broad_ancestral_category")

    def countryOfOrigin = column[Option[String]]("country_of_origin", O.Default(None))

    def countryOfRecruitment = column[Option[String]]("country_of_recruitment", O.Default(None))

    def numberOfIndividuals = column[Int]("number_of_individuals")

    //def sourceId = column[String]("ancestry_source_id", O.Unique)
    def sourceId = column[String]("ancestry_source_id")

    def cohort = foreignKey("ancestries_cohort_fk", cohortId, cohorts)(
      _.cohortId,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

    def * = (ancestryId.?, cohortId, broadAncestralCategory, countryOfOrigin, countryOfRecruitment, numberOfIndividuals, sourceId)
  }

  val ancestries = TableQuery[Ancestry]

  class Donors(tag: Tag) extends
    Table[(Option[Int], String, Option[String], Option[Int], Option[Int], Option[String], Option[String], Option[Int], Option[String])](tag, DONOR_TABLE_NAME) {
    def donorId = column[Int]("donor_id", O.PrimaryKey, O.AutoInc)

    def sourceId = column[String]("donor_source_id", O.Unique)

    def species = column[Option[String]]("species", O.Default(None))

    def speciesTid = column[Option[Int]]("species_tid", O.Default(None))

    def age = column[Option[Int]]("age", O.Default(None))

    def gender = column[Option[String]]("gender", O.Default(None))

    def ethnicity = column[Option[String]]("ethnicity", O.Default(None))

    def ethnicityTid = column[Option[Int]]("ethnicity_tid", O.Default(None))

    def altDonorSourceId = column[Option[String]]("alt_donor_source_id", O.Default(None))

    def * = (donorId.?, sourceId, species, speciesTid, age, gender, ethnicity, ethnicityTid, altDonorSourceId)
  }

  val donors = TableQuery[Donors]

  class BioSamples(tag: Tag) extends
    Table[(Option[Int], Int, String, Option[String], Option[String], Option[Int], Option[String], Option[Int], Option[Boolean], Option[String], Option[Int], Option[String])](tag, BIOSAMPLE_TABLE_NAME) {
    def bioSampleId = column[Int]("biosample_id", O.PrimaryKey, O.AutoInc)

    def donorId = column[Int]("donor_id")

    def sourceId = column[String]("biosample_source_id", O.Unique)

    def types = column[Option[String]]("biosample_type", O.Default(None))

    def tissue = column[Option[String]]("tissue", O.Default(None))

    def tissueTid = column[Option[Int]]("tissue_tid", O.Default(None))

    def cell = column[Option[String]]("cell", O.Default(None))

    def cellTid = column[Option[Int]]("cell_tid", O.Default(None))

    def isHealthy = column[Option[Boolean]]("is_healthy", O.Default(None))

    def disease = column[Option[String]]("disease", O.Default(None))

    def diseaseTid = column[Option[Int]]("disease_tid", O.Default(None))

    def altBiosampleSourceId = column[Option[String]]("alt_biosample_source_id", O.Default(None))

    def donor = foreignKey("biosamples_donor_fk", donorId, donors)(
      _.donorId,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

    def * = (bioSampleId.?, donorId, sourceId, types, tissue, tissueTid, cell, cellTid, isHealthy, disease, diseaseTid, altBiosampleSourceId)
  }

  val bioSamples = TableQuery[BioSamples]

  class Replicates(tag: Tag) extends
    Table[(Option[Int], Int, String, Option[Int], Option[Int])](tag, REPLICATE_TABLE_NAME) {
    def replicateId = column[Int]("replicate_id", O.PrimaryKey, O.AutoInc)

    def bioSampleId = column[Int]("biosample_id")

    def sourceId = column[String]("replicate_source_id", O.Unique)

    def bioReplicateNum = column[Option[Int]]("biological_replicate_number", O.Default(None))

    def techReplicateNum = column[Option[Int]]("technical_replicate_number", O.Default(None))

    def bioSample = foreignKey("replicates_biosample_fk", bioSampleId, bioSamples)(
      _.bioSampleId,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

    def * = (replicateId.?, bioSampleId, sourceId, bioReplicateNum, techReplicateNum)
  }

  val replicates = TableQuery[Replicates]

  class ExperimentsType(tag: Tag) extends
    Table[(Option[Int], Option[String], Option[Int], Option[String], Option[Int], Option[String], Option[Int], Option[String])](tag, EXPERIMENTTYPE_TABLE_NAME) {
    def experimentTypeId = column[Int]("experiment_type_id", O.PrimaryKey, O.AutoInc)

    def technique = column[Option[String]]("technique", O.Default(None))

    def techniqueTid = column[Option[Int]]("technique_tid", O.Default(None))

    def feature = column[Option[String]]("feature", O.Default(None))

    def featureTid = column[Option[Int]]("feature_tid", O.Default(None))

    def target = column[Option[String]]("target", O.Default(None))

    def targetTid = column[Option[Int]]("target_tid", O.Default(None))

    def antibody = column[Option[String]]("antibody", O.Default(None))

    def uniqueKey = index("technique_feature_target", (technique, feature, target), unique = true)

    def * = (experimentTypeId.?, technique, techniqueTid, feature, featureTid, target, targetTid, antibody)
  }

  val experimentsType = TableQuery[ExperimentsType]

  class Projects(tag: Tag) extends
    Table[(Option[Int], String, Option[String])](tag, PROJECT_TABLE_NAME) {
    def projectId = column[Int]("project_id", O.PrimaryKey, O.AutoInc)

    def projectName = column[String]("project_name", O.Unique)

    def programName = column[Option[String]]("source", O.Default(None))

    def * = (projectId.?, projectName, programName)
  }

  val projects = TableQuery[Projects]

  class Cases(tag: Tag) extends
    Table[(Option[Int], Int, String, Option[String], Option[String], Option[String])](tag, CASE_TABLE_NAME) {
    def caseId = column[Int]("case_study_id", O.PrimaryKey, O.AutoInc)

    def projectId = column[Int]("project_id")

    def sourceId = column[String]("case_source_id", O.Unique)

    def sourceSite = column[Option[String]]("source_site", O.Default(None))

    def externalRef = column[Option[String]]("external_reference", O.Default(None))

    def altCaseSourceId = column[Option[String]]("alt_case_source_id", O.Default(None))

    def project = foreignKey("cases_project_fk", projectId, projects)(
      _.projectId,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

    def * = (caseId.?, projectId, sourceId, sourceSite, externalRef, altCaseSourceId)
  }

  val cases = TableQuery[Cases]

  class Datasets(tag: Tag) extends
    Table[(Option[Int], String, Option[String], Option[String], Option[String], Option[Boolean])](tag, DATASET_TABLE_NAME) {
    def datasetId = column[Int]("dataset_id", O.PrimaryKey, O.AutoInc)

    def name = column[String]("dataset_name", O.Unique)

    def dataType = column[Option[String]]("data_type", O.Default(None))

    def format = column[Option[String]]("file_format", O.Default(None))

    def assembly = column[Option[String]]("assembly", O.Default(None))

    def isAnn = column[Option[Boolean]]("is_annotation", O.Default(None))

    def * = (datasetId.?, name, dataType, format, assembly, isAnn)
  }

  val datasets = TableQuery[Datasets]

  class Items(tag: Tag) extends
    Table[(Option[Int], Int, Int, String, Option[Long], Option[String], Option[String], Option[String], Option[Int],
      Option[String], Option[Int], Option[String], Option[String], Option[String], Option[String],
      Option[String], Option[String])](tag, ITEM_TABLE_NAME) {

    def itemId = column[Int]("item_id", O.PrimaryKey, O.AutoInc)

    def experimentTypeId = column[Int]("experiment_type_id")

    def datasetId = column[Int]("dataset_id")

    def sourceId = column[String]("item_source_id", O.Unique)

    def size = column[Option[Long]]("size", O.Default(None))

    def date = column[Option[String]]("date", O.Default(None))

    def checksum = column[Option[String]]("checksum", O.Default(None))

    def contentType = column[Option[String]]("content_type", O.Default(None))

    def contentTypeTid = column[Option[Int]]("content_type_tid", O.Default(None))

    def platform = column[Option[String]]("platform", O.Default(None))

    def platformTid = column[Option[Int]]("platform_tid", O.Default(None))

    def pipeline = column[Option[String]]("pipeline", O.Default(None))

    def sourceUrl = column[Option[String]]("source_url", O.Default(None))

    def localUrl = column[Option[String]]("local_url", O.Default(None))

    def fileName = column[Option[String]]("file_name", O.Default(None))

    def sourcePage = column[Option[String]]("source_page", O.Default(None))

    def altItemSourceId = column[Option[String]]("alt_item_source_id", O.Default(None))

    def experimentType = foreignKey("items_experiment_type_fk", experimentTypeId, experimentsType)(
      _.experimentTypeId,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

    def dataset = foreignKey("items_dataset_fk", datasetId, datasets)(
      _.datasetId,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

    def * = (itemId.?, experimentTypeId, datasetId, sourceId, size, date, checksum, contentType, contentTypeTid,
      platform, platformTid, pipeline, sourceUrl, localUrl, fileName, sourcePage, altItemSourceId)
  }

  val items = TableQuery[Items]

  class CasesItems(tag: Tag) extends
    Table[(Int, Int)](tag, CASEITEM_TABLE_NAME) {
    def itemId = column[Int]("item_id")

    def caseId = column[Int]("case_study_id")

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

  /*class DerivedFrom(tag: Tag) extends
    Table[(Int, Int, Option[String])](tag, DERIVEDFROM_TABLE_NAME) {

    def initialItemId = column[Int]("initial_item_id")

    def finalItemId = column[Int]("final_item_id")

    def operation = column[Option[String]]("operation", O.Default(None))

    def pk = primaryKey("item_replicate_id_derivedfrom", (initialItemId, finalItemId))

    def initialItemIdFK = foreignKey("items_initialitem_fk", initialItemId, items)(
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

  val derivedFrom = TableQuery[DerivedFrom]*/

  class PairTable(tag: Tag) extends
    Table[(Int, String, String)](tag, PAIR_TABLE_NAME) {
    def itemId = column[Int]("item_id")

    def key = column[String]("key")

    def value = column[String]("value")

    def pk = primaryKey("item_id_key_value", (itemId, key, value))

    def itemIdFK = foreignKey("items_item_id_fk", itemId, items)(
      _.itemId,
      onUpdate = ForeignKeyAction.Restrict,
      onDelete = ForeignKeyAction.Cascade
    )

    def * = (itemId, key, value)
  }

  val pairs = TableQuery[PairTable]


  //-------------------------------------DATABASE DW VIEWS---------------------------------------------------------------


  def setDWViews(): Unit = {
    val createSchemaDW = sqlu"""CREATE SCHEMA IF NOT EXISTS dw;"""
    val setupcreatedw = database.run(createSchemaDW)
    Await.result(setupcreatedw, Duration.Inf)
    logger.info("dw schema created")

    val dropReplicateViewDW = sqlu"""DROP VIEW IF EXISTS dw.replicate CASCADE;"""
    val setupdropreplicate = database.run(dropReplicateViewDW)
    Await.result(setupdropreplicate, Duration.Inf)
    logger.info("dw.replicate view dropped")

    val replicateViewTry = Try {
      val createReplicateViewDW =
        sqlu"""CREATE VIEW dw.replicate AS
               SELECT replicate_id,
               biosample_id,
               replicate_source_id,
               biological_replicate_number,
              biological_replicate_number || '_' || technical_replicate_number as technical_replicate_number
              from replicate;"""
      val result = database.run(createReplicateViewDW)
      Await.result(result, Duration.Inf)
    } match {
      case Success(value) => logger.info("dw.replicate view created")
      case Failure(f) => {
        logger.info("SQL query for dw.replicate generated an error", f)
        throw new Exception("SQL query for dw.replicate generated an error")
      }
    }

    val dropItemViewDW = sqlu"""DROP MATERIALIZED VIEW IF EXISTS dw.item CASCADE;"""
    val setupdropitem = database.run(dropItemViewDW)
    Await.result(setupdropitem, Duration.Inf)
    logger.info("dw.item view dropped")

    val itemViewTry = Try {
      val createItemViewDW =
        sqlu"""CREATE MATERIALIZED VIEW dw.item AS SELECT
                                  (SELECT COUNT(distinct biological_replicate_number)
                                    FROM replicate2item
                                    NATURAL JOIN dw.replicate
                                    WHERE item.item_id = replicate2item.item_id)
                                  as biological_replicate_count,
                                  (SELECT COUNT(distinct r.technical_replicate_number)
                                  FROM replicate2item
                                  NATURAL JOIN dw.replicate as r
                                  WHERE item.item_id = replicate2item.item_id)
                                  as technical_replicate_count,
                                  *
                                  FROM item;"""
      val result = database.run(createItemViewDW)
      Await.result(result, Duration.Inf)
    } match {
      case Success(value) => logger.info("dw.item view created")
      case Failure(f) => {
        logger.info("SQL query for dw.item generated an error", f)
        throw new Exception("SQL query for dw.item generated an error")
      }
    }

    val itemViewIndexesTry = Try {
      val createItemViewIndexesDW =
        sqlu"""CREATE INDEX ON dw.item (lower(platform));
                CREATE INDEX ON dw.item (lower(pipeline));
                CREATE INDEX ON dw.item (lower(content_type));
                CREATE INDEX ON dw.item (biological_replicate_count);
                CREATE INDEX ON dw.item (technical_replicate_count);
                CREATE INDEX ON dw.item (item_id);
                CREATE INDEX ON dw.item (alt_item_source_id);"""
      val result = database.run(createItemViewIndexesDW)
      Await.result(result, Duration.Inf)
    } match {
      case Success(value) => logger.info("dw.item view indexes created")
      case Failure(f) => {
        logger.info("SQL query for dw.item indexes generated an error", f)
        throw new Exception("SQL query for dw.item indexes generated an error")
      }
    }

  }




  def setFlattenMaterialized(): Unit = {

    val dropFlattenViewDW = sqlu"""DROP MATERIALIZED VIEW IF EXISTS dw.flatten CASCADE;"""
    val setupdropflatten = database.run(dropFlattenViewDW)
    Await.result(setupdropflatten, Duration.Inf)
    logger.info("dw.flatten dropped")

    val flattenViewTry = Try {
      val createFlattenViewDW =
        sqlu"""CREATE MATERIALIZED VIEW dw.flatten AS
      SELECT DISTINCT
      item_id                                          AS item_id,
      LOWER(biosample_type)                                      AS biosample_type,
      LOWER(tissue)                                    AS tissue,
      LOWER(cell)                                 AS cell,
      (is_healthy)                                     AS is_healthy,
      LOWER(disease)                                   AS disease,
      LOWER(source_site)                               AS source_site,
      LOWER(external_reference)                              AS external_reference,
      LOWER(dataset_name)                                      AS dataset_name,
      LOWER(data_type)                                 AS data_type,
      LOWER(file_format)                                    AS file_format,
      LOWER(assembly)                                  AS assembly,
      (is_annotation)                                         AS is_annotation,
      LOWER(species)                                   AS species,
      (age)                                            AS age,
      LOWER(gender)                                    AS gender,
      LOWER(ethnicity)                                 AS ethnicity,
      LOWER(technique)                                 AS technique,
      LOWER(feature)                                   AS feature,
      LOWER(target)                                    AS target,
      LOWER(antibody)                                  AS antibody,
      LOWER(platform)                                  AS platform,
      LOWER(pipeline)                                  AS pipeline,
      LOWER(content_type)                              AS content_type,
      LOWER(source)                              AS source,
      LOWER(project_name)                              AS project_name,
      biological_replicate_number                              AS biological_replicate_number,
      technical_replicate_number AS technical_replicate_number,
      biological_replicate_count                       AS biological_replicate_count,
      technical_replicate_count                        AS technical_replicate_count
      FROM dw.item i
        NATURAL JOIN dataset d
      NATURAL JOIN experiment_type et
      NATURAL JOIN case2item c2i
      NATURAL JOIN case_study cs
      NATURAL JOIN project p
      NATURAL JOIN replicate2item r2i
      NATURAL JOIN dw.replicate r
      NATURAL JOIN biosample b
      NATURAL JOIN donor d2
      ;"""
      val setupcreateflatten = database.run(createFlattenViewDW)
      Await.result(setupcreateflatten, Duration.Inf)
    } match {
      case Success(value) => logger.info("dw.flatten created")
      case Failure(f) => {
        logger.info("SQL query for dw.flatten generated an error", f)
        throw new Exception("SQL query for dw.flatten generated an error")
      }
    }

    val flattenIndexesTry = Try {
      val createFlattenIndexesDW =
        sqlu"""CREATE INDEX dw_flatten_biosample_type_idx ON dw.flatten (biosample_type);
                CREATE INDEX dw_flatten_tissue_idx ON dw.flatten (tissue);
                CREATE INDEX dw_flatten_cell_idx ON dw.flatten (cell);
                CREATE INDEX dw_flatten_is_healthy_idx ON dw.flatten (is_healthy);
                CREATE INDEX dw_flatten_disease_idx ON dw.flatten (disease);
                CREATE INDEX dw_flatten_source_site_idx ON dw.flatten (source_site);
                CREATE INDEX dw_flatten_external_reference_idx ON dw.flatten (external_reference);
                CREATE INDEX dw_flatten_dataset_name_idx ON dw.flatten (dataset_name);
                CREATE INDEX dw_flatten_data_type_idx ON dw.flatten (data_type);
                CREATE INDEX dw_flatten_file_format_idx ON dw.flatten (file_format);
                CREATE INDEX dw_flatten_assembly_idx ON dw.flatten (assembly);
                CREATE INDEX dw_flatten_is_annotation_idx ON dw.flatten (is_annotation);
                CREATE INDEX dw_flatten_species_idx ON dw.flatten (species);
                CREATE INDEX dw_flatten_age_idx ON dw.flatten (age);
                CREATE INDEX dw_flatten_gender_idx ON dw.flatten (gender);
                CREATE INDEX dw_flatten_ethnicity_idx ON dw.flatten (ethnicity);
                CREATE INDEX dw_flatten_technique_idx ON dw.flatten (technique);
                CREATE INDEX dw_flatten_feature_idx ON dw.flatten (feature);
                CREATE INDEX dw_flatten_target_idx ON dw.flatten (target);
                CREATE INDEX dw_flatten_antibody_idx ON dw.flatten (antibody);
                CREATE INDEX dw_flatten_platform_idx ON dw.flatten (platform);
                CREATE INDEX dw_flatten_pipeline_idx ON dw.flatten (pipeline);
                CREATE INDEX dw_flatten_content_type_idx ON dw.flatten (content_type);
                CREATE INDEX dw_flatten_source_idx ON dw.flatten (source);
                CREATE INDEX dw_flatten_project_name_idx ON dw.flatten (project_name);
                CREATE INDEX dw_flatten_biological_replicate_number_idx ON dw.flatten (biological_replicate_number);
                CREATE INDEX dw_flatten_technical_replicate_number_idx ON dw.flatten (technical_replicate_number);
                CREATE INDEX dw_flatten_biological_replicate_count_idx ON dw.flatten (biological_replicate_count);
                CREATE INDEX dw_flatten_technical_replicate_count_idx ON dw.flatten (technical_replicate_count);"""
      val setupcreateindex = database.run(createFlattenIndexesDW)
      Await.result(setupcreateindex, Duration.Inf)
    } match {
      case Success(value) => logger.info("dw.flatten indexes created")
      case Failure(f) => {
        logger.info("SQL query for INDEXES for dw.flatten generated an error", f)
        throw new Exception("SQL query for INDEXES for dw.flatten generated an error")
      }
    }
  }

  def refreshFlattenMaterialized(): Unit = {

    val refreshFlattenViewDW = sqlu"""REFRESH MATERIALIZED VIEW dw.flatten;"""
    val setuprefreshflatten = database.run(refreshFlattenViewDW)
    Await.result(setuprefreshflatten, Duration.Inf)
    logger.info("dw.flatten refreshed")

  }

  def refreshItemMaterialized(): Unit = {

    val refreshItemViewDW = sqlu"""REFRESH MATERIALIZED VIEW dw.item;"""
    val setuprefreshitem = database.run(refreshItemViewDW)
    Await.result(setuprefreshitem, Duration.Inf)
    logger.info("dw.item refreshed")

  }

  //-------------------------------------UNIFIED PAIR---------------------------------------------------------------


  def setUnifiedPair(): Unit = {

    val dropGcmPairView = sqlu"""DROP MATERIALIZED VIEW IF EXISTS gcm_pair CASCADE;"""
    val setupdropGcmPair = database.run(dropGcmPairView)
    Await.result(setupdropGcmPair, Duration.Inf)
    logger.info("gcm_pair dropped")

    val gcmPairViewTry = Try {
      val createGcmPairView =
        sqlu"""CREATE MATERIALIZED VIEW gcm_pair AS(
              select item_id, 'biosample_type' as key, biosample_type as value, 'biosample' as table_name
              from dw.flatten
              union
              select item_id, 'tissue' as key, tissue as value, 'biosample' as table_name
              from dw.flatten
              union
              select item_id, 'cell' as key, cell as value, 'biosample' as table_name
              from dw.flatten
              union
              select item_id, 'disease' as key, disease as value, 'biosample' as table_name
              from dw.flatten
              union
              select item_id, 'is_healthy' as key, is_healthy::text as value, 'biosample' as table_name
              from dw.flatten
              union
              select item_id, 'source_site' as key, source_site as value, 'case' as table_name
              from dw.flatten
              union
              select item_id, 'external_reference' as key, external_reference as value, 'case' as table_name
              from dw.flatten
              union
              select item_id, 'dataset_name' as key, dataset_name as value, 'dataset' as table_name
              from dw.flatten
              union
              select item_id, 'data_type' as key, data_type as value, 'dataset' as table_name
              from dw.flatten
              union
              select item_id, 'file_format' as key, file_format as value, 'dataset' as table_name
              from dw.flatten
              union
              select item_id, 'assembly' as key, assembly as value, 'dataset' as table_name
              from dw.flatten
              union
              select item_id, 'is_annotation' as key, is_annotation::text as value, 'dataset' as table_name
              from dw.flatten
              union
              select item_id, 'species' as key, species as value, 'donor' as table_name
              from dw.flatten
              union
              select item_id, 'age' as key, age::text as value, 'donor' as table_name
              from dw.flatten
              union
              select item_id, 'gender' as key, gender as value, 'donor' as table_name
              from dw.flatten
              union
              select item_id, 'ethnicity' as key, ethnicity as value, 'donor' as table_name
              from dw.flatten
              union
              select item_id, 'technique' as key, technique as value, 'experiment_type' as table_name
              from dw.flatten
              union
              select item_id, 'feature' as key, feature as value, 'experiment_type' as table_name
              from dw.flatten
              union
              select item_id, 'target' as key, target as value, 'experiment_type' as table_name
              from dw.flatten
              union
              select item_id, 'antibody' as key, antibody as value, 'experiment_type' as table_name
              from dw.flatten
              union
              select item_id, 'pipeline' as key, pipeline as value, 'item' as table_name
              from dw.flatten
              union
              select item_id, 'platform' as key, platform as value, 'item' as table_name
              from dw.flatten
              union
              select item_id, 'content_type' as key, content_type as value, 'item' as table_name
              from dw.flatten
              union
              select item_id, 'project_name' as key, project_name as value, 'project' as table_name
              from dw.flatten
              union
              select item_id, 'source' as key, source as value, 'project' as table_name
              from dw.flatten
              union
              select item_id,
                     'biological_replicate_number' as key,
                     biological_replicate_number::text as value,
                     'replicate'                   as table_name
              from dw.flatten
              union
              select item_id, 'technical_replicate_number' as key, technical_replicate_number as value, 'replicate' as table_name
              from dw.flatten
              union
              select item_id, 'biological_replicate_count' as key, biological_replicate_count::text as value, 'replicate' as table_name
              from dw.flatten
              union
              select item_id, 'technical_replicate_count' as key, technical_replicate_count::text as value, 'replicate' as table_name
              from dw.flatten
        );"""
      val setupcreateGcmPair = database.run(createGcmPairView)
      Await.result(setupcreateGcmPair, Duration.Inf)
    } match {
      case Success(value) => logger.info("gcm_pair created")
      case Failure(f) => {
        logger.info("SQL query for gcm_pair generated an error", f)
        throw new Exception("SQL query for gcm_pair generated an error")
      }
    }

    val dropUnifiedPair = sqlu"""DROP MATERIALIZED VIEW IF EXISTS unified_pair CASCADE;"""
    val setupdropUnifiedPair = database.run(dropUnifiedPair)
    Await.result(setupdropUnifiedPair, Duration.Inf)
    logger.info("unified_pair dropped")

    val UnifiedPairCreateTry: Unit = Try {
      val createUnifiedPair =
        sqlu"""CREATE MATERIALIZED VIEW unified_pair
                AS
                select item_id,
               lower(key)::varchar(128)   as key,
               lower(value) as value,
               false        as is_gcm
                from pair
                UNION
                select item_id,
               lower(key)::varchar(128)   as key,
               lower(value) as value,
               true         as is_gcm
               from gcm_pair;"""
      val result = database.run(createUnifiedPair)
      Await.result(result, Duration.Inf)
    } match {
      case Success(value) => logger.info("unified_pair created")
      case Failure(f) => {
        logger.info("SQL query for unified_pair generated an error", f)
        throw new Exception("SQL query for unified_pair generated an error")
      }
    }

    val UnifiedPairInsertTry: Unit = Try {
      val insertUnifiedPair =
        sqlu"""CREATE UNIQUE INDEX unified_pair_unique_idx
                ON unified_pair (item_id, key, value, is_gcm);"""
      val result = database.run(insertUnifiedPair)
      Await.result(result, Duration.Inf)
    } match {
      case Success(value) => logger.info("constraint in unified_pair completed")
      case Failure(f) => {
        logger.info("constraint creation in unified_pair generated an error", f)
        throw new Exception("constraint creation in unified_pair generated an error")
      }
    }


    val unifiedPairIndexesTry = Try {
      val unifiedPairIndexes =
        sqlu"""CREATE INDEX ON unified_pair (item_id);
                CREATE INDEX ON unified_pair USING GIN (item_id,lower(key));
                CREATE INDEX ON unified_pair USING GIN (item_id,lower(value));
                CREATE INDEX ON unified_pair USING GIN (lower(key),item_id);
                CREATE INDEX ON unified_pair USING GIN (lower(value),item_id);
                CREATE INDEX ON unified_pair USING GIN (item_id,key);
                CREATE INDEX ON unified_pair USING GIN (item_id,value);
                CREATE INDEX ON unified_pair USING GIN (key,item_id);
                CREATE INDEX ON unified_pair USING GIN (value,item_id);
                CREATE INDEX ON unified_pair USING GIN (lower(key));
                CREATE INDEX ON unified_pair USING GIN (lower(value));
                CREATE INDEX ON unified_pair USING GIN (key);
                CREATE INDEX ON unified_pair USING GIN (value);"""
      val setupUPindex = database.run(unifiedPairIndexes)
      Await.result(setupUPindex, Duration.Inf)
    } match {
      case Success(value) => logger.info("unified_pair indexes created")
      case Failure(f) => {
        logger.info("SQL query for INDEXES for unified_pair generated an error", f)
        throw new Exception("SQL query for INDEXES for unified_pair generated an error")
      }
    }
  }

  def fixGENCODEUrlProblem(): Unit = {

    val fixGENCODEUrlProblemTry = Try {
      val updateGENCODEUrl =
        sqlu"""UPDATE item
              SET source_url = regexp_replace(source_url,'(ftp://ftp.)sanger(.ac.uk/pub/)(.*)','\1ebi\2databases/\3')
              where dataset_id in (select dataset_id from dataset where dataset_name ilike '%_GENCODE')
              and source_url ilike 'ftp://ftp.sanger.ac.uk%';"""
      val result = database.run(updateGENCODEUrl)
      Await.result(result, Duration.Inf)
    } match {
      case Success(value) => logger.info("Update on GENCODE source_url finished")
      case Failure(f) => {
        logger.info("Update on GENCODE source_url generated an error", f)
        throw new Exception("Update on GENCODE source_url generated an error")
      }
    }

  }

}
