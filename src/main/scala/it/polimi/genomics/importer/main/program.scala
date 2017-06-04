package it.polimi.genomics.importer.main

import java.io.File
import scala.concurrent.{blocking, Future, Await}
import scala.concurrent.duration._


import it.polimi.genomics.importer.FileDatabase.FileDatabase
import it.polimi.genomics.importer.GMQLImporter._
import it.polimi.genomics.importer.GMQLImporter.utils.SCHEMA_LOCATION
import it.polimi.genomics.repository.Utilities
import org.apache.log4j._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.concurrent.Future
import scala.xml.{Elem, XML}

object program {
  val logger: Logger = Logger.getLogger(this.getClass)

  /**
    * depending on the arguments, can run download/transform/load procedure or
    * delete transformed folder.
    * @param args arguments for GMQLImporter (help for more information)
    */
  def main(args: Array[String]): Unit = {

    val console = new ConsoleAppender() //create appender
    //configure the appender
    val PATTERN = "%d [%p|%c|%C{1}] %m%n"
    console.setLayout(new PatternLayout(PATTERN))
    console.setThreshold(Level.ERROR)
    console.activateOptions()
    //add appender to any Logger (here is root)
    Logger.getRootLogger.addAppender(console)
    val console2 = new ConsoleAppender()
    //configure the appender
    console2.setLayout(new PatternLayout(PATTERN))
    console2.setThreshold(Level.INFO)
    console2.activateOptions()
    Logger.getLogger("it.polimi.genomics.importer").addAppender(console2)

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
          + "\t\t-Shows log and statistics for all runs\n"
          + "\t Add log -n\n"
          + "\t\t-Shows log and statistics run n\n"
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
            val run =
              if(args.exists(arg => {arg.contains("-")&& arg.length<5}))
                args.filter(arg => {arg.contains("-")&& arg.length<5}).head.replace("-","")
              else
                "0"
            try{
              val runId = Integer.parseInt(run)
              showDatabaseLog(args.head, args.drop(1).head.toString,runId)
            }
            catch {
              case e: NumberFormatException =>
                logger.error("Defined run must be integer value.")
            }
          }
          else {
            runGMQLImporter(args.head, args.drop(1).head.toString,args.contains("-retry"))
          }
        }
        else
          logger.warn("No configuration file or gmql_conf folder specified")
      }
    }
  }

  /**
    * Shows the log of past runs for download and transformations.
    * @param xmlConfigPath path to xml configuration file containing the sources and general configuration.
    * @param gmqlConfigPath path to gmql_config folder.
    * @param runId identifier of the run to show the run, for showing all of them enter 0.
    */
  def showDatabaseLog(xmlConfigPath: String, gmqlConfigPath: String,runId : Int): Unit = {
    Utilities.confFolder = new File(gmqlConfigPath).getAbsolutePath
    //general settings
    if (new File(xmlConfigPath).exists()) {
      val schemaUrl =
        "https://raw.githubusercontent.com/DEIB-GECO/GMQL-Importer/master/Example/xml/configurationSchema.xsd"
      if (schemaValidator.validate(xmlConfigPath, schemaUrl)) {

        logger.info("Xml file is valid for the schema")
        val file: Elem = XML.loadFile(xmlConfigPath)
        val outputFolder = (file \\ "settings" \ "base_working_directory").text

        //start database
        FileDatabase.setDatabase(outputFolder)
        //load sources
        val sources = loadSources(xmlConfigPath)
        //here I have to print all download and transform logs
        if(runId == 0){
          val runNumbers = FileDatabase.getMaxRunNumber
          logger.info("Printing all runs' details")
          for(i <- 1 until runNumbers){
            logger.info(s"Printing details for run $i")
            sources.foreach(source => {
              logger.info(s"Source: ${source.name}")
              val sourceId = FileDatabase.sourceId(source.name)
              source.datasets.foreach(dataset => {
                val datasetId = FileDatabase.datasetId(sourceId, dataset.name)
                val runDatasetId = FileDatabase.runDatasetId(
                  i,
                  datasetId,
                  dataset.outputFolder,
                  dataset.downloadEnabled.toString,
                  dataset.transformEnabled.toString,
                  dataset.loadEnabled.toString,
                  dataset.schemaUrl,
                  dataset.schemaLocation.toString
                )
                if(runDatasetId!=0) {
                  FileDatabase.printRunDatasetDownloadLog(runDatasetId)
                  FileDatabase.printRunDatasetTransformLog(runDatasetId)
                }
              })
            })
          }
        }
        //here I print just the detail of the selected run
        else{
          logger.info(s"Printing details for run $runId")
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
              FileDatabase.printRunDatasetDownloadLog(runDatasetId)
              FileDatabase.printRunDatasetTransformLog(runDatasetId)
            })
          })

        }
        //close database
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
    * @param xmlConfigPath xml configuration file location
    * @param gmqlConfigPath path to the gmql_conf folder
    */
  def runGMQLImporter(xmlConfigPath: String, gmqlConfigPath: String, retryDownload: Boolean): Unit = {
    Utilities.confFolder = new File(gmqlConfigPath).getAbsolutePath
    //general settings
    if (new File(xmlConfigPath).exists()) {
      val schemaUrl =
        "https://raw.githubusercontent.com/DEIB-GECO/GMQL-Importer/master/Example/xml/configurationSchema.xsd"
      if (schemaValidator.validate(xmlConfigPath, schemaUrl)) {

        logger.info("Xml file is valid for the schema")
        val file: Elem = XML.loadFile(xmlConfigPath)
        val outputFolder = (file \\ "settings" \ "base_working_directory").text

        val downloadEnabled =
          if ("true".equalsIgnoreCase((file \\ "settings" \ "download_enabled").text)) true else false
        val transformEnabled =
          if ("true".equalsIgnoreCase((file \\ "settings" \ "transform_enabled").text)) true else false
        val loadEnabled =
          if ("true".equalsIgnoreCase((file \\ "settings" \ "load_enabled").text)) true else false
        val parallelExecution =
          if ("true".equalsIgnoreCase((file \\ "settings" \ "parallel_execution").text)) true else false
          if(loadEnabled){

        }
        //start database
        FileDatabase.setDatabase(outputFolder)
        //load sources
        val sources = loadSources(xmlConfigPath)
        //start run
        val runId = FileDatabase.runId(
          downloadEnabled.toString, transformEnabled.toString, loadEnabled.toString, outputFolder)

        //creation of the file log
        val logName = "run "+runId+" "+DateTime.now.toString(DateTimeFormat.forPattern("yyyy_MM_dd HH:mm:ss.SSS Z"))+".log"
        val fa = new FileAppender()
        fa.setName("FileLogger")
        fa.setFile(logName)
        fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"))
        fa.setThreshold(Level.WARN)
        fa.setAppend(true)
        fa.activateOptions()
        Logger.getRootLogger.addAppender(fa)

        val fa2 = new FileAppender()
        fa2.setName("FileLogger")
        fa2.setFile(logName)
        fa2.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"))
        fa2.setThreshold(Level.TRACE)
        fa2.setAppend(true)
        fa2.activateOptions()
        Logger.getLogger("it.polimi.genomics.importer").addAppender(fa2)
        //to continue consistency of xml configuration is needed.
        if(checkConsistencyConfigurationXml(sources,loadEnabled,transformEnabled)) {
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

          val downloadThreads = sources.filter(_.downloadEnabled && downloadEnabled ).map( source =>{
            new Thread {
              override def run(): Unit = {
                  if (!retryDownload) {
                    logger.info(s"Starting download for ${source.name}")
                    Class.forName(source.downloader).newInstance.asInstanceOf[GMQLDownloader].download(source,parallelExecution)
                    logger.info(s"Download for ${source.name} Finished")
                  }
                  else {
                    logger.info(s"Retrying failed downloads for ${source.name}")
                    Class.forName(source.downloader).newInstance.asInstanceOf[GMQLDownloader].downloadFailedFiles(source,parallelExecution)
                    logger.info(s"Download for ${source.name} Finished")
                  }
              }
            }
          })
          if(parallelExecution) {
            downloadThreads.foreach(_.start())
            downloadThreads.foreach(_.join())
          }
          else
            downloadThreads.foreach(thread => {
              thread.start()
              thread.join()
            })


          val integrateThreads = sources.filter(_.transformEnabled && transformEnabled).map( source =>{
            new Thread {
              override def run(): Unit = {
                  logger.info(s"Starting integration for ${source.name}")
                  Integrator.integrate(source, parallelExecution)
                  logger.info(s"Integration for ${source.name} Finished")
              }
            }
          })
          if(parallelExecution) {
          integrateThreads.foreach(_.start())
          integrateThreads.foreach(_.join())
          }
          else
            integrateThreads.foreach(thread => {
              thread.start()
              thread.join()
            })

          sources.filter(_.loadEnabled && loadEnabled).foreach(source => {
              logger.info(s"Starting load for ${source.name}")
              Class.forName(source.loader).newInstance.asInstanceOf[GMQLLoader].loadIntoGMQL(source)
              logger.info(s"Loading for ${source.name} Finished")
          })
          sources.foreach(source => {
            val sourceId = FileDatabase.sourceId(source.name)
            logger.info(s"Log for source: ${source.name}")
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
              if (runDatasetId != -1) {
                if (downloadEnabled && source.downloadEnabled && dataset.downloadEnabled) {
                  FileDatabase.printRunDatasetDownloadLog(runDatasetId)
                }
                if (transformEnabled && source.transformEnabled && dataset.transformEnabled) {
                  FileDatabase.printRunDatasetTransformLog(runDatasetId)
                }
              }
            })
          })
        }
        else{
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

  /**
    * Checks consistency warnings in the xml configuration file and the metadata replacement.
    * asks the user if continue or not with the execution.
    * @param sources sequence of sources to be checked
    * @param loadEnabled indicates if load is enables on the root level of xmlConfigurationFile
    * @param transformEnabled indicates if transformation is enabled on the root level of xmlConfigurationFile
    * @return true means to follow the execution.
    */
  def checkConsistencyConfigurationXml(sources: Seq[GMQLSource], loadEnabled:Boolean, transformEnabled: Boolean): Boolean ={
    //check metadata replacement
    if(transformEnabled)
    sources.foreach(source =>{
      if(source.transformEnabled && source.parameters.exists(_._1=="metadata_replacement"))
        //They are already forbidding to create new dataset with the same name.
          source.parameters.filter(_._1=="metadata_replacement").foreach(file =>{
          val replacements = (XML.loadFile(source.rootOutputFolder+File.separator+file._2)\\"replace").map(_.text.replace(' ','_').toLowerCase)
          replacements.map(replacement => {
            (replacement,replacements.count(_==replacement))
          }).filter(_._2>1).foreach(fail =>{
            logger.warn(file._2+" has "+fail._2+" replacements that have the same output: "+fail._1+
              ".\nWould you like to continue anyway? (Y/n):")
            // readLine lets you prompt the user and read their input as a String
            scala.Console.flush()
            val input = scala.Console.in.readLine()
//            val input = scala.io.StdIn.readLine()
            if(input.toLowerCase == "n")
              return false
          })
        })
      if(loadEnabled)
      source.datasets.filter(_.loadEnabled && source.loadEnabled).foreach(dataset =>{
        val datasetName =
          if(dataset.parameters.exists(_._1=="loading_name"))
            dataset.parameters.filter(_._1=="loading_name").head._2
          else
            source.name + "_" + dataset.name
      })
    })
    true
  }

  /**
    * from xmlConfigPath loads the sources there defined. All folder paths defined inside sources and
    * datasets are referred from the base root output folder (working directory).
    * @param xmlConfigPath xml configuration file location
    * @return Seq of sources with their respective datasets and settings.
    */
  def loadSources(xmlConfigPath: String): Seq[GMQLSource]={
    val file: Elem = XML.loadFile(xmlConfigPath)
    val outputFolder = (file \\ "settings" \ "base_working_directory").text
    //load sources
    val sources = (file \\ "source_list" \ "source").map(source => {
      GMQLSource(
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
          ((parameter \ "key").text, (parameter \ "value").text,(parameter \ "description").text, (parameter \ "type").text)
        }),
        (source \ "dataset_list" \ "dataset").map(dataset => {
          GMQLDataset(
            (dataset \ "@name").text,
            (dataset \ "dataset_working_directory").text,
            (dataset \ "schema_url").text,
            SCHEMA_LOCATION.withName((dataset \ "schema_url" \ "@location").text),
            if ((dataset \ "download_enabled").text.toLowerCase == "true") true else false,
            if ((dataset \ "transform_enabled").text.toLowerCase == "true") true else false,
            if ((dataset \ "load_enabled").text.toLowerCase == "true") true else false,
            (dataset \ "parameter_list" \ "parameter").map(parameter => {
              ((parameter \ "key").text, (parameter \ "value").text,(parameter \ "description").text , (parameter \ "type").text)
            })
          )
        })
      )
    })
    sources
  }
}

