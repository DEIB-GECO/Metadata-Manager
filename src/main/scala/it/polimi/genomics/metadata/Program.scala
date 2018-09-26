package it.polimi.genomics.metadata

import java.io.File

import it.polimi.genomics.metadata.downloader_transformer.Downloader
import it.polimi.genomics.metadata.step.utils.ExecutionLevel.{ExecutionLevel, _}
import it.polimi.genomics.metadata.step._
import it.polimi.genomics.metadata.step.utils.{ParameterUtil, SchemaLocation, SchemaValidator}
import it.polimi.genomics.metadata.database.FileDatabase
import it.polimi.genomics.metadata.step.xml.{Dataset, Source}
import it.polimi.genomics.repository.Utilities
import org.apache.log4j._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.xml.{Elem, XML}


/**
  * Handles the execution of the GMQLImporter, gets the arguments and process them.
  * Created by Nacho
  */
object Program extends App {
  val logger: Logger = Logger.getLogger(this.getClass)
  val PATTERN = "%d [%p] - %l %m%n"


  val console = new ConsoleAppender() //create appender
  //configure the appender
  console.setLayout(new PatternLayout(PATTERN))
  console.setThreshold(Level.WARN)
  console.activateOptions()
  //add appender to any Logger (here is root)
  Logger.getRootLogger.addAppender(console)
  val console2 = new ConsoleAppender()
  //configure the appenderN
  console2.setLayout(new PatternLayout(PATTERN))
  console2.setThreshold(Level.DEBUG)
  console2.activateOptions()
  Logger.getLogger("it.polimi.genomics").addAppender(console2)

  val console3 = new ConsoleAppender()
  //configure the appender
  console3.setLayout(new PatternLayout(PATTERN))
  console3.setThreshold(Level.WARN)
  console3.activateOptions()
  Logger.getLogger("slick").addAppender(console3)

  //    BasicConfigurator.configure()
  if (args.length == 0) {
    logger.info("no arguments specified, run with help for more information")
  }
  else {
    if (args.contains("help")) {
      logger.info("GMQLImporter help:\n"
        + "\t Run with configuration_xml_path and gmql_conf_folder as arguments\n"
        + "\t\t -Will run whole process defined in xml file\n"
        + "\t Add log\n"
        + "\t\t-Shows how many runs the program has executed\n"
        + "\t Add log -n\n"
        + "\t\t-Shows log and statistics n-th last runs, 1 for last run and use comma as separator if multiple runs requested\n"
        + "\t Add -retry\n"
        + "\t\t-Tries to download failed files only for each dataset with download enabled\n"
      )
    }
    else if (args.length > 1) {
      val xmlPath = args.head
      val gmqlConfPath = args.drop(1).head
      logger.info(s"path for the xml file = $xmlPath")
      logger.info(s"path for the gmql Configuration folder = $gmqlConfPath")
      if (xmlPath.endsWith(".xml") && new File(gmqlConfPath).isDirectory) {
        if (args.contains("log")) {
          val runs =
            if (args.exists(arg => {
              arg.contains("--")
            }))
              args.filter(arg => {
                arg.contains("--")
              }).head.replace("--", "")
            else
              "0"
          try {
            for (run <- runs.split(",")) {
              val runId = Integer.parseInt(run)
              showDatabaseLog(args.head, args.drop(1).head.toString, runId)
            }
          }
          catch {
            case e: NumberFormatException =>
              logger.warn("Defined run must be integer value.")
          }
        }
        else {
          val t0: Long = System.nanoTime()
          runGMQLImporter(args.head, args.drop(1).head.toString, args.contains("-retry"))
          val t1 = System.nanoTime()
          logger.info(s"Total time for the run ${getTotalTimeFormatted(t0, t1)}")
        }
      }
      else
        logger.warn("No configuration file or gmql_conf folder specified")
    }
  }


  /**
    * gets the time between 2 timestamps in hh:mm:ss format
    *
    * @param t0 start time
    * @param t1 end time
    * @return hh:mm:ss as string
    */
  def getTotalTimeFormatted(t0: Long, t1: Long): String = {

    val hours = Integer.parseInt("" + (t1 - t0) / 1000000000 / 60 / 60)
    val minutes = Integer.parseInt("" + ((t1 - t0) / 1000000000 / 60 - hours * 60))
    val seconds = Integer.parseInt("" + ((t1 - t0) / 1000000000 - hours * 60 * 60 - minutes * 60))
    s"$hours:$minutes:$seconds"
  }

  /**
    * Shows the log of past runs for download and transformations.
    *
    * @param xmlConfigPath  path to xml configuration file containing the sources and general configuration.
    * @param gmqlConfigPath path to gmql_config folder.
    * @param runId          identifier of the run to show the run, for showing all of them enter 0.
    */
  def showDatabaseLog(xmlConfigPath: String, gmqlConfigPath: String, runId: Int): Unit = {
    Utilities.confFolder = new File(gmqlConfigPath).getAbsolutePath
    //general settings
    if (new File(xmlConfigPath).exists()) {
      val schemaUrl =
        "https://raw.githubusercontent.com/DEIB-GECO/GMQL-Importer/master/Example/xml/configurationSchema.xsd"
      if (SchemaValidator.validate(xmlConfigPath, schemaUrl)) {

        val file: Elem = XML.loadFile(xmlConfigPath)
        val outputFolder = (file \\ "settings" \ "base_working_directory").text
        ParameterUtil.gcmConfigFile = (file \\ "settings" \ "gcm_config_file").text

        //start database
        FileDatabase.setDatabase(outputFolder)
        var run = FileDatabase.getMaxRunNumber
        if (runId <= run) {
          for (i <- 1 until runId)
            run = FileDatabase.getPreviousRunNumber(run)
          if (run > 0 && runId != 0) {
            //load sources
            val sources = loadSources(xmlConfigPath)
            //here I print just the detail of the selected run
            logger.info(s"Printing details for run $run")
            sources.foreach(source => {
              logger.info(s"Source: ${source.name}")
              val sourceId = FileDatabase.sourceId(source.name)
              source.datasets.foreach(dataset => {
                val datasetId = FileDatabase.datasetId(sourceId, dataset.name)
                val runDatasetId = FileDatabase.runDatasetId(
                  runId,
                  datasetId,
                  dataset.outputFolder,
                  dataset.downloadEnabled.toString,
                  dataset.transformEnabled.toString,
                  dataset.loadEnabled.toString,
                  dataset.schemaUrl,
                  dataset.schemaLocation.toString
                )
                FileDatabase.printRunDatasetDownloadLog(runDatasetId, datasetId, run)
                FileDatabase.printRunDatasetTransformLog(runDatasetId)
              })
            })
            //close database
          }
          else {
            logger.info(s"Actually ${FileDatabase.getNumberOfRuns} runs have been executed")
          }
        }
        else
          logger.info(s"Requested nº$runId but only $run executions exist")
        FileDatabase.closeDatabase()
      }
      else
        logger.warn("Xml file is not valid according the specified schema, check: " + schemaUrl)
    }
    else
      logger.warn(xmlConfigPath + " does not exist")
  }

  /**
    * by having a configuration xml file runs downloaders/transformers/loader for the sources and their
    * datasets if defined to.
    *
    * @param xmlConfigPath  xml configuration file location
    * @param gmqlConfigPath path to the gmql_conf folder
    * @param retryDownload  defines if the execution is only for retrying failed files or normal execution
    */
  def runGMQLImporter(xmlConfigPath: String, gmqlConfigPath: String, retryDownload: Boolean): Unit = {
    Utilities.confFolder = new File(gmqlConfigPath).getAbsolutePath
    //general settings
    if (new File(xmlConfigPath).exists()) {
      val schemaUrl =
        "https://raw.githubusercontent.com/DEIB-GECO/GMQL-Importer/master/Example/xml/configurationSchema.xsd"
      if (SchemaValidator.validate(xmlConfigPath, schemaUrl)) {

        logger.info("Xml file is valid for the schema")
        val file: Elem = XML.loadFile(xmlConfigPath)
        val outputFolder = (file \\ "settings" \ "base_working_directory").text

        val downloadEnabled =
          if ("true".equalsIgnoreCase((file \\ "settings" \ "download_enabled").text)) true else false
        val transformEnabled =
          if ("true".equalsIgnoreCase((file \\ "settings" \ "transform_enabled").text)) true else false

        val cleanerEnabled =
          if ("true".equalsIgnoreCase((file \\ "settings" \ "cleaner_enabled").text)) true else false
        val mapperEnabled =
          if ("true".equalsIgnoreCase((file \\ "settings" \ "mapper_enabled").text)) true else false
        val enricherEnabled =
          if ("true".equalsIgnoreCase((file \\ "settings" \ "enricher_enabled").text)) true else false
        val flattenerEnabled =
          if ("true".equalsIgnoreCase((file \\ "settings" \ "flattener_enabled").text)) true else false

        val loadEnabled =
          if ("true".equalsIgnoreCase((file \\ "settings" \ "load_enabled").text)) true else false
        val parallelExecution =
          if ("true".equalsIgnoreCase((file \\ "settings" \ "parallel_execution").text)) true else false
        if (loadEnabled) {

        }
        //start database
        FileDatabase.setDatabase(outputFolder)
        //load sources
        val sources = loadSources(xmlConfigPath)
        //start run
        val runId = FileDatabase.runId(
          downloadEnabled.toString, transformEnabled.toString, loadEnabled.toString, outputFolder)

        //creation of the file log
        val logName = "run_" + runId + "_" + DateTime.now.toString(DateTimeFormat.forPattern("yyyy_MM_dd_HH_mm_ss_SSS")) + ".log"


        val fa2 = new FileAppender()
        fa2.setName("FileLogger")
        fa2.setFile(logName)
        fa2.setLayout(new PatternLayout(PATTERN))
        fa2.setThreshold(Level.DEBUG)
        fa2.setAppend(true)
        fa2.activateOptions()
        Logger.getLogger("it.polimi.genomics.importer").addAppender(fa2)
        //to continue consistency of xml configuration is needed.
        if (checkConsistencyConfigurationXml(sources, loadEnabled, transformEnabled)) {
          //start DTL
          sources.foreach(source => {
            val sourceId = FileDatabase.sourceId(source.name)
            val runSourceId = FileDatabase.runSourceId(
              runId,
              sourceId,
              source.url,
              source.outputFolder,
              source.downloadEnabled.toString,
              source.downloader,
              source.transformEnabled.toString,
              source.transformer,
              source.loadEnabled.toString,
              source.loader
            )
            source.parameters.foreach(parameter => {
              FileDatabase.runSourceParameterId(runSourceId, parameter._3, parameter._1, parameter._2)
            })
            source.datasets.foreach(dataset => {
              val datasetId = FileDatabase.datasetId(sourceId, dataset.name)
              val runDatasetId = FileDatabase.runDatasetId(
                runId,
                datasetId,
                dataset.outputFolder,
                dataset.downloadEnabled.toString,
                dataset.transformEnabled.toString,
                dataset.loadEnabled.toString,
                dataset.schemaUrl,
                dataset.schemaLocation.toString
              )
              dataset.parameters.foreach(parameter => {
                FileDatabase.runDatasetParameterId(runDatasetId, parameter._3, parameter._1, parameter._2)
              })
            })
          })


          if (downloadEnabled)
            executeDownload(sources, parallelExecution, retryDownload)
          if (transformEnabled)
            executeLevel(sources, Transform, parallelExecution)
          if (cleanerEnabled)
            executeLevel(sources, Clean, parallelExecution)


          if (loadEnabled) {
            sources.filter(_.loadEnabled && loadEnabled).foreach(source => {
              logger.info(s"Starting load for ${source.name}")
              Class.forName(source.loader).newInstance.asInstanceOf[GMQLLoader].loadIntoGMQL(source)
              logger.info(s"Loading for ${source.name} Finished")
            })
          }
          val t4: Long = System.nanoTime()
          sources.foreach(source => {
            val t0Source = System.nanoTime()
            val sourceId = FileDatabase.sourceId(source.name)
            if (source.loadEnabled || source.transformEnabled || source.downloadEnabled)
              logger.info(s"Statistics for source: ${source.name}")
            source.datasets.foreach(dataset => {
              val t0Dataset = System.nanoTime()

              val datasetId = FileDatabase.datasetId(sourceId, dataset.name)
              val runDatasetId = FileDatabase.runDatasetId(
                runId,
                datasetId,
                dataset.outputFolder,
                dataset.downloadEnabled.toString,
                dataset.transformEnabled.toString,
                dataset.loadEnabled.toString,
                dataset.schemaUrl,
                dataset.schemaLocation.toString
              )
              if (runDatasetId != -1) {
                if (downloadEnabled && source.downloadEnabled && dataset.downloadEnabled) {
                  FileDatabase.printRunDatasetDownloadLog(runDatasetId, datasetId, runId)
                }
                if (transformEnabled && source.transformEnabled && dataset.transformEnabled) {
                  FileDatabase.printRunDatasetTransformLog(runDatasetId)
                }
              }
              val t1Dataset = System.nanoTime()
              if (dataset.loadEnabled && source.loadEnabled && loadEnabled)
                logger.info(s"Total time load dataset ${dataset.name}: ${getTotalTimeFormatted(t0Dataset, t1Dataset)}")
            })
            val t1Source = System.nanoTime()
            if (source.loadEnabled && loadEnabled)
              logger.info(s"Total time load source ${source.name}: ${getTotalTimeFormatted(t0Source, t1Source)}")
          })
          val t5 = System.nanoTime()
          if (loadEnabled)
            logger.info(s"Total time for loads: ${getTotalTimeFormatted(t4, t5)}")
        }
        else {
          logger.info("The user has canceled the run")
        }
        //end DTL

        //end database run
        FileDatabase.endRun(runId)
        //close database

        FileDatabase.closeDatabase()
      }
      else
        logger.warn("Xml file is not valid according the specified schema, check: " + schemaUrl)
    }
    else
      logger.warn(xmlConfigPath + " does not exist")
  }


  def executeDownload(sources: Seq[Source], parallelExecution: Boolean, retryDownload: Boolean) = {
    val downloadThreads = sources.filter(_.downloadEnabled).map(source => {
      new Thread {
        override def run(): Unit = {
          val t0Source = System.nanoTime()
          if (!retryDownload) {
            logger.info(s"Starting download for ${source.name}")
            Class.forName(source.downloader).newInstance.asInstanceOf[Downloader].download(source, parallelExecution)
            logger.info(s"Download for ${source.name} Finished")
          }
          else {
            logger.info(s"Retrying failed downloads for ${source.name}")
            Class.forName(source.downloader).newInstance.asInstanceOf[Downloader].downloadFailedFiles(source, parallelExecution)
            logger.info(s"Download for ${source.name} Finished")
          }
          val t1Source = System.nanoTime()
          logger.info(s"Total time download source ${source.name}: ${getTotalTimeFormatted(t0Source, t1Source)}")
        }
      }
    })
    val t0: Long = System.nanoTime()
    if (parallelExecution) {
      downloadThreads.foreach(_.start())
      downloadThreads.foreach(_.join())
    }
    else
      downloadThreads.foreach(thread => {
        thread.start()
        thread.join()
      })
    val t1 = System.nanoTime()
    logger.info(s"Total time for downloads: ${getTotalTimeFormatted(t0, t1)}")

  }

  def executeLevel(sources: Seq[Source], level: ExecutionLevel, parallelExecution: Boolean): Unit = {
    val integrateThreads = sources.filter(_.isEnabled(level)).map(source => {
      new Thread {
        override def run(): Unit = {
          val t0Source = System.nanoTime()
          logger.info(s"Starting $level for ${source.name}")
          Step.getLevelExecutable(level).execute(source, parallelExecution)
          logger.info(s"$level for ${source.name} Finished")
          val t1Source = System.nanoTime()
          logger.info(s"Total time $level source ${source.name}: ${getTotalTimeFormatted(t0Source, t1Source)}")
        }
      }
    })
    val t2: Long = System.nanoTime()
    if (parallelExecution) {
      integrateThreads.foreach(_.start())
      integrateThreads.foreach(_.join())
    }
    else
      integrateThreads.foreach(thread => {
        thread.start()
        thread.join()
      })
    val t3 = System.nanoTime()
    logger.info(s"Total time for transformations: ${getTotalTimeFormatted(t2, t3)}")
  }

  /**
    * Checks consistency warnings in the xml configuration file and the metadata replacement.
    * asks the user if continue or not with the execution.
    *
    * @param sources          sequence of sources to be checked
    * @param loadEnabled      indicates if load is enables on the root level of xmlConfigurationFile
    * @param transformEnabled indicates if transformation is enabled on the root level of xmlConfigurationFile
    * @return true means to follow the execution.
    */
  def checkConsistencyConfigurationXml(sources: Seq[Source], loadEnabled: Boolean, transformEnabled: Boolean): Boolean = {
    //check metadata replacement
    if (transformEnabled)
      sources.foreach(source => {
        if (source.transformEnabled && source.parameters.exists(_._1 == "metadata_replacement"))
        //They are already forbidding to create new dataset with the same name.
          source.parameters.filter(_._1 == "metadata_replacement").foreach(file => {
            val replacements = (XML.loadFile(source.rootOutputFolder + File.separator + file._2) \\ "replace").map(_.text.replace(' ', '_').toLowerCase)
            replacements.map(replacement => {
              (replacement, replacements.count(_ == replacement))
            }).filter(_._2 > 1).foreach(fail => {
              logger.warn(file._2 + " has " + fail._2 + " replacements that have the same output: " + fail._1 +
                ".\nWould you like to continue anyway? (Y/n):")
              // readLine lets you prompt the user and read their input as a String
              scala.Console.flush()
              val input = scala.Console.in.readLine()
              //            val input = scala.io.StdIn.readLine()
              if (input.toLowerCase == "n")
                return false
            })
          })
        if (loadEnabled)
          source.datasets.filter(_.loadEnabled && source.loadEnabled).foreach(dataset => {
            val datasetName =
              if (dataset.parameters.exists(_._1 == "loading_name"))
                dataset.parameters.filter(_._1 == "loading_name").head._2
              else
                source.name + "_" + dataset.name
          })
      })
    true
  }

  /**
    * from xmlConfigPath loads the sources there defined. All folder paths defined inside sources and
    * datasets are referred from the base root output folder (working directory).
    *
    * @param xmlConfigPath xml configuration file location
    * @return Seq of sources with their respective datasets and settings.
    */
  def loadSources(xmlConfigPath: String): Seq[Source] = {
    val file: Elem = XML.loadFile(xmlConfigPath)
    val outputFolder = (file \\ "settings" \ "base_working_directory").text
    //load sources
    val sources = (file \\ "source_list" \ "source").map { source =>
      val gmqlSource = Source(
        (source \ "@name").text,
        (source \ "url").text,
        outputFolder + File.separator + (source \ "source_working_directory").text,
        outputFolder,
        if ((source \ "download_enabled").text.toLowerCase == "true") true else false,
        (source \ "downloader").text,
        if ((source \ "transform_enabled").text.toLowerCase == "true") true else false,
        (source \ "transformer").text,
        if ((source \ "load_enabled").text.toLowerCase == "true") true else false,
        (source \ "loader").text,
        (source \ "parameter_list" \ "parameter").map(parameter => {
          ((parameter \ "key").text, (parameter \ "value").text, (parameter \ "description").text, (parameter \ "type").text)
        }),
        (source \ "dataset_list" \ "dataset").map(dataset => {
          Dataset(
            (dataset \ "@name").text,
            (dataset \ "dataset_working_directory").text,
            (dataset \ "schema_url").text,
            SchemaLocation.withName((dataset \ "schema_url" \ "@location").text),
            if ((dataset \ "download_enabled").text.toLowerCase == "true") true else false,
            if ((dataset \ "transform_enabled").text.toLowerCase == "true") true else false,
            if ((dataset \ "load_enabled").text.toLowerCase == "true") true else false,
            (dataset \ "parameter_list" \ "parameter").map(parameter => {
              ((parameter \ "key").text, (parameter \ "value").text, (parameter \ "description").text, (parameter \ "type").text)
            })
          )
        }),
        if ((source \ "cleaner_enabled").text.toLowerCase == "true") true else false,
        if ((source \ "mapper_enabled").text.toLowerCase == "true") true else false,
        if ((source \ "enricher_enabled").text.toLowerCase == "true") true else false,
        if ((source \ "flattener_enabled").text.toLowerCase == "true") true else false
      )
      gmqlSource.datasets.foreach(_.source = gmqlSource)
      gmqlSource
    }
    sources
  }
}

