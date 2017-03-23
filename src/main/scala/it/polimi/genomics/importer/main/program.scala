package it.polimi.genomics.importer.main

import java.io.File

import it.polimi.genomics.importer.FileDatabase.FileDatabase
import it.polimi.genomics.importer.GMQLImporter._
import it.polimi.genomics.importer.GMQLImporter.utils.SCHEMA_LOCATION
import org.apache.log4j._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

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

//    BasicConfigurator.configure()
    if(args.length == 0){
      logger.info("no arguments specified, run with help for more information")
    }
    else{
      if(args.contains("help")){
        logger.info("GMQLImporter help:\n"
          +"\t Run with configuration_xml_path as argument\n"
          +"\t\t -Will run whole process defined in xml file\n"
          +"\t Run with configuration_xml_path dt\n"
          +"\t\t-Will delete Transformations folder for datasets\n"
          +"\t Run with configuration_xml_path log\n"
          +"\t\t-Shows log and statistics for the last run\n"
          +"\t Run with configuration_xml_path log -f\n"
          +"\t\t-Shows log and statistics for every run\n"
          +"\t\t defined to transform in configuration xml\n"
        )
      }
/*      else if(args.contains("dt")){
        if(args.head.endsWith(".xml"))
          deleteTransformations(args.head)
        else
          logger.warn("No configuration file specified")
      }*/
      else if(args.contains("log")){
        if(args.head.endsWith(".xml")){
          if(args.contains("-f")){
            showDatabaseLog(args.head,fullReport = true)
          }
          else
            showDatabaseLog(args.head,fullReport = false)
        }
        else
          logger.warn("No configuration file specified")
      }
      else if(args.head.endsWith(".xml")){
        run(args.head)
      }
    }
  }

  /**
    * displays the log and statistics on the database, it can be done for the last run or a full report for all the runs
    * @param xmlConfigPath xml configuration file location
    * @param fullReport decides if is a report of the last run or all the runs
    */
  def showDatabaseLog(xmlConfigPath: String, fullReport: Boolean): Unit = {
    //general settings
    if (new File(xmlConfigPath).exists()) {
      val schemaUrl =
        "https://raw.githubusercontent.com/nachodox/utils/master/configurationSchema.xsd"
      if (schemaValidator.validate(xmlConfigPath, schemaUrl)) {
        logger.info("Xml file is valid for the schema")

        val file: Elem = XML.loadFile(xmlConfigPath)
        val outputFolder = (file \\ "settings" \ "base_working_directory").text
        //        val logProperties = (file \\ "settings" \ "logger_properties").text
        //now logger loaded programatically
        //        try {
        //          DOMConfigurator.configure(logProperties)
        //        }
        //        catch {
        //          case e: Exception =>
        //            logger.warn("Configuration file for log4j is not valid. Log will be performed with basic configuration")
        //        }

        //start database
        FileDatabase.setDatabase(outputFolder)
        //load sources
        val sources = loadSources(xmlConfigPath)
        //start run
        val runId = FileDatabase.getMaxRunNumber

        val logName = "run." + runId + "." + DateTime.now.toString(DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss.SSS Z")) + ".log"
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
    */
  def run(xmlConfigPath: String): Unit = {
    //general settings
    if (new File(xmlConfigPath).exists()) {
      val schemaUrl =
        "https://raw.githubusercontent.com/nachodox/utils/master/configurationSchema.xsd"
      if (schemaValidator.validate(xmlConfigPath, schemaUrl)) {

        val file: Elem = XML.loadFile(xmlConfigPath)
        val outputFolder = (file \\ "settings" \ "base_working_directory").text
//        val logProperties = (file \\ "settings" \ "logger_properties").text
//now logger loaded programatically
//        try {
//          DOMConfigurator.configure(logProperties)
//        }
//        catch {
//          case e: Exception =>
//            logger.warn("Configuration file for log4j is not valid. Log will be performed with basic configuration")
//        }

        logger.info("Xml file is valid for the schema")

        val downloadEnabled =
          if ("true".equalsIgnoreCase((file \\ "settings" \ "download_enabled").text)) true else false
        val transformEnabled =
          if ("true".equalsIgnoreCase((file \\ "settings" \ "transform_enabled").text)) true else false
        val loadEnabled =
          if ("true".equalsIgnoreCase((file \\ "settings" \ "load_enabled").text)) true else false
        //start database
        FileDatabase.setDatabase(outputFolder)
        //load sources
        val sources = loadSources(xmlConfigPath)
        //start run
        val runId = FileDatabase.runId(
          downloadEnabled.toString, transformEnabled.toString, loadEnabled.toString, outputFolder)


        val logName = "run."+runId+"."+DateTime.now.toString(DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss.SSS Z"))+".log"
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
            if (downloadEnabled && source.downloadEnabled) {
              logger.info(s"Starting download for ${source.name}")
              Class.forName(source.downloader).newInstance.asInstanceOf[GMQLDownloader].download(source)
              logger.info(s"Download for ${source.name} Finished")
            }
            if (transformEnabled && source.transformEnabled) {
              logger.info(s"Starting integration for ${source.name}")
              Integrator.integrate(source)
              logger.info(s"Integration for ${source.name} Finished")
            }
            if (loadEnabled && source.loadEnabled) {
              logger.info(s"Starting load for ${source.name}")
              Class.forName(source.loader).newInstance.asInstanceOf[GMQLLoader].loadIntoGMQL(source)
              logger.info(s"Loading for ${source.name} Finished")
            }
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
    * @return true means to follow the execution.
    */
  def checkConsistencyConfigurationXml(sources: Seq[GMQLSource], loadEnabled:Boolean, transformEnabled: Boolean): Boolean ={
    //check metadata replacement
    if(transformEnabled)
    sources.foreach(source =>{
      if(source.transformEnabled && source.parameters.exists(_._1=="metadata_replacement"))
        source.parameters.filter(_._1=="metadata_replacement").foreach(file =>{
          val replacements = (XML.loadFile(source.rootOutputFolder+File.separator+file._2)\\"replace").map(_.text.replace(' ','_').toLowerCase)
          replacements.map(replacement => {
            (replacement,replacements.count(_==replacement))
          }).filter(_._2>1).foreach(fail =>{
            logger.warn(file._2+" has "+fail._2+" replacements that have the same output: "+fail._1+
              ".\nWould you like to continue anyway? (Y/n):")
            // readLine lets you prompt the user and read their input as a String
            scala.Console.flush()
            val input = readLine
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
        source.parameters.filter(_._1=="gmql_user").foreach(user =>{
          if(Class.forName(source.loader).newInstance.asInstanceOf[GMQLLoader].dsExists(user._2,datasetName)){
            logger.warn("Dataset "+datasetName+" already exists for user "+user._2+" it will be replaced."+
              ".\nWould you like to continue anyway? (Y/n):")
            // readLine lets you prompt the user and read their input as a String
            scala.Console.flush()
            val input = readLine
            if(input.toLowerCase == "n")
              return false
          }
        })
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
          ((parameter \ "key").text, (parameter \ "value").text,(parameter \ "description").text)
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
              ((parameter \ "key").text, (parameter \ "value").text,(parameter \ "description").text )
            })
          )
        }),
        (source \ "dataset_list" \ "merged_dataset").map(dataset => {
          GMQLDataset(
            (dataset \ "@name").text,
            (dataset \ "output_folder").text,
            "",//because merged schema has to be created.
            SCHEMA_LOCATION.LOCAL,
            downloadEnabled = false,//I dont use download. and I use transform to put the merge.
            transformEnabled = if ((dataset \ "merge_enabled").text.toLowerCase == "true") true else false,
            loadEnabled = if ((dataset \ "load_enabled").text.toLowerCase == "true") true else false,
            (dataset \ "origin_dataset_list" \ "origin_dataset").map(parameter => {
              ("origin_dataset", parameter.text,"asdas")
            })
          )
        })
      )
    })
    sources
  }

  /**
    * deletes folders of transformations on the transformation_enabled datasets.
    * @param xmlConfigPath xml configuration file location
    */
  def deleteTransformations(xmlConfigPath: String): Unit = {
    try {
      if (new File(xmlConfigPath).exists()) {
        val file: Elem = XML.loadFile(xmlConfigPath)
        val transformEnabled =
          if ("true".equalsIgnoreCase((file \\ "settings" \ "transform_enabled").text)) true else false
        if (transformEnabled) {
          val sources = loadSources(xmlConfigPath)
          sources.foreach(source => {
            if (source.transformEnabled) source.datasets.foreach(dataset => {
              if (dataset.transformEnabled) {
                val path = source.outputFolder + File.separator + dataset.outputFolder + File.separator + "Transformations"
                try {
                  val folder = new File(path)
                  if (folder.exists()) {
                    deleteFolder(folder)
                    logger.info("Folder: " + path + " DELETED")
                  }
                }
                catch {
                  case ex: NullPointerException => logger.error(s"couldn't delete transformation folder $path: ${ex.getMessage}")
                  case ex: SecurityException => logger.error(s"security problems with $path: ${ex.getMessage}")
                }
              }
            })
          })
        }
      }
      else
        logger.warn(xmlConfigPath + " does not exist")
    }
    catch {
      case ex: NullPointerException => logger.error(s"couldn't check xml configuration path $xmlConfigPath: ${ex.getMessage}")
      case ex: SecurityException => logger.error(s"security problems with $xmlConfigPath: ${ex.getMessage}")
    }
  }

  /**
    * deletes folder recursively
    * @param path base folder path
    */
  def deleteFolder(path: File): Unit ={
    try {
      if (path.exists()) {
        try {
          val files = path.listFiles()
          files.foreach(file => {
            try {
              if (file.isDirectory)
                deleteFolder(file)
              else
                try {
                  file.delete()
                }
                catch {
                  case ex: SecurityException => logger.warn(s"Couldn't delete $path: ${ex.getMessage}")
                }
            }
            catch {
              case ex: SecurityException => logger.warn(s"Couldn't access $path: ${ex.getMessage}")
            }
          })
        }
        catch {
          case ex: SecurityException => logger.warn(s"Couldn't list files from $path: ${ex.getMessage}")
        }
      }
    }
    catch {
      case ex: SecurityException => logger.warn(s"Couldn't delete folder $path: ${ex.getMessage}")
    }
  }
}

