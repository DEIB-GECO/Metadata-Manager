package it.polimi.genomics.importer.ModelDatabase


import it.polimi.genomics.importer.ModelDatabase.Utils._
import java.io._

import com.typesafe.config.ConfigFactory
import it.polimi.genomics.importer.RemoteDatabase.DbHandler
import it.polimi.genomics.importer.main.program.getTotalTimeFormatted
import org.apache.log4j._
import it.polimi.genomics.importer.GMQLImporter.schemaValidator
import it.polimi.genomics.importer.ModelDatabase.Encode.Utils.{BioSampleList, PlatformRetriver, ReplicateList}
import it.polimi.genomics.importer.ModelDatabase.Encode.{EncodeTableId, EncodeTables}
import it.polimi.genomics.importer.ModelDatabase.TCGA.TCGATables
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.io.Source


object main{
  val logger: Logger = Logger.getLogger(this.getClass)
  private val regexBedMetaJson = ".*.bed.meta.json".r
  private val regexBedMeta = ".*.bed.meta\\z".r
  private val regexBedMetaTCGA = ".*.bed.meta".r

  private val exportRegexENCODE = "bed.meta.json".r
  private val exportRegexTCGA = "bed.meta".r

  private val encodeString = "ENCODE"
  private val tcgaString = "TCGA"

  var filePath: String = _

  val conf = ConfigFactory.load()

  def main(args: Array[String]): Unit = {
    val console = new ConsoleAppender() //create appender
    //configure the appender
    val PATTERN = "%d [%p|%c|%C{1}] %m%n"
    /*console.setLayout(new PatternLayout(PATTERN))
    console.setThreshold(Level.ERROR)
    console.activateOptions()
    //add appender to any Logger (here is root)
    Logger.getRootLogger.addAppender(console)*/
    val console2 = new ConsoleAppender()

    //configure the appender
    console2.setLayout(new PatternLayout(PATTERN))
    console2.setThreshold(Level.INFO)
    console2.activateOptions()
    Logger.getLogger("it.polimi.genomics.importer").addAppender(console2)

    //configure the appender
    val console3 = new ConsoleAppender()
    console3.setLayout(new PatternLayout(PATTERN))
    console3.setThreshold(Level.WARN)
    console3.activateOptions()
    Logger.getLogger("slick").addAppender(console3)

    //BasicConfigurator.configure()

    var states = collection.mutable.Map[String, String]()

     DbHandler.setDatabase()


     if (args.length == 0) {
       logger.warn(s"No arguments specified")
       return
     }
     if( args.length < 3 || args.length > 4){
       logger.error(s"Incorrect number of arguments:")
       logger.info("GMQLImporter help; in order to import Data from File:\n"
         + "\t Run with configuration_xml_path, file_folder, repository_ref and IMPORT as arguments\n"
       )
       logger.info("GMQLImporter help; in order to export Data from Database to file:\n"
         + "\t Run with file_folder, repository_ref and EXPORT as arguments\n"
       )
       return
     }
     if(args(0).toUpperCase != "IMPORT" && args(0).toUpperCase != "EXPORT") {
       logger.error(s"Incorrect execution_mode argument")
       logger.info("Please select 'import' or 'export'")
       return
     }
     if (args(0).toUpperCase.equals("IMPORT")) {
       if (args.length != 4) {
         logger.error(s"Incorrect number of arguments for IMPORT execution mode")
         logger.info("GMQLImporter help; in order to import Data from File:\n"
           + "\t Run with configuration_xml_path, file_folder, repository_ref and IMPORT as arguments\n"
         )
         return
       }
       if(args(1).toUpperCase != encodeString && args(1).toUpperCase != tcgaString ){
         logger.error(s"Incorrect repository argument")
         logger.info("Please select 'encode' or 'tcga'")
         return
       }
       importModality(args(1), args(2), args(3))
     } else {
       if (args.length != 3) {
         logger.error(s"Incorrect number of arguments")
         logger.info("GMQLImporter help; in order to export Data from Database to file:\n"
           + "\t Run with EXPORT, repository_ref and file_folder as arguments\n"
         )
         return
       }
       exportModality(args(1), args(2))
     }
     DbHandler.closeDatabase()
  }


  def importModality(repositoryRef: String, pathGMQL: String, pathXML: String): Unit = {
    val schemaUrl = "https://raw.githubusercontent.com/DEIB-GECO/GMQL-Importer/federico_merged/Example/xml/setting.xsd"
    if (schemaValidator.validate(pathXML, schemaUrl)) {
      logger.info("Xml file is valid for the schema")
      DbHandler.setDatabase()

      val logName = "IMPORT_" + repositoryRef.toUpperCase + "_" + DateTime.now.toString(DateTimeFormat.forPattern("yyyy_MM_dd HH:mm:ss.SSS Z")) + ".log"

      defineFileAppenderSetting(logName)

      val t0: Long = System.nanoTime()
      repositoryRef.toUpperCase() match {
        case "ENCODE" => ListFiles.recursiveListFiles(new File(pathGMQL)).filter(f => regexBedMetaJson.findFirstIn(f.getName).isDefined).map(path => analizeFileEncode(path.toString, pathXML))
        case "TCGA" => ListFiles.recursiveListFiles(new File(pathGMQL)).filter(f => regexBedMetaTCGA.findFirstIn(f.getName).isDefined).map(path => analizeFileTCGA(path.toString, pathXML))
        case _ => logger.error(s"Incorrect repository")
      }
      val t1 = System.nanoTime()
      logger.info(s"Total time to insert data in DB ${getTotalTimeFormatted(t0, t1)}")
      logger.info(s"Total analyzed files ${Statistics.fileNumber}")
      logger.info(s"Total file released ${Statistics.released}")
      logger.info(s"File status (and other metadata) are missing ${Statistics.archived}")
      logger.info(s"Total file released but not inserted ${Statistics.releasedItemNotInserted}")
      logger.info(s"Total Item inserted ${Statistics.itemInserted}")
      logger.info(s"Total Item updated ${Statistics.itemUpdated}")
      logger.info(s"Constraints violated ${Statistics.constraintsViolated}")
      logger.info(s"Total malformation found  ${Statistics.malformedInput}")
      logger.info(s"ArrayIndexOutOfBoundsException file input  ${Statistics.indexOutOfBoundsException}")
      logger.info(s"UnknownInputException file input  ${Statistics.anotherInputException}")
      logger.info(s"Extracted Time  ${Statistics.getTimeFormatted(Statistics.extractTimeAcc)}")
      logger.info(s"Transform Time  ${Statistics.getTimeFormatted(Statistics.transformTimeAcc)}")
      logger.info(s"Load Time  ${Statistics.getTimeFormatted(Statistics.loadTimeAcc)}")
      if(repositoryRef.toUpperCase.equals("ENCODE")) {
        logger.info(s"Total Donor inserted or updated ${Statistics.donorInsertedOrUpdated}")
        logger.info(s"Total Biosample inserted or updated ${Statistics.biosampleInsertedOrUpdated}")
        logger.info(s"Total Replicate inserted or updated ${Statistics.replicateInsertedOrUpdated}")

      }


    }
  }

  def exportModality(repositoryRef: String, pathGMQL: String): Unit = {

    val logName = "EXPORT_" + repositoryRef.toUpperCase + "_" + DateTime.now.toString(DateTimeFormat.forPattern("yyyy_MM_dd HH:mm:ss.SSS Z"))+".log"

    defineFileAppenderSetting(logName)

    logger.info(s"Start to write TSV file")
    val t2: Long = System.nanoTime()
    val fromDbToTsv = new FromDbToTsv()
    repositoryRef.toUpperCase() match {
      case "ENCODE" => {

        ListFiles.recursiveListFiles(new File(pathGMQL)).filter(f => regexBedMetaJson.findFirstIn(f.getName).isDefined).map(path => {
          val tables = new EncodeTables(new EncodeTableId).getListOfTables()
          fromDbToTsv.setTable(tables._1, tables._2, tables._3, tables._4, tables._5, tables._6, tables._7, tables._8)
          fromDbToTsv.run(path.getAbsolutePath, exportRegexENCODE)
        })
      }
      case "TCGA" =>  {
        ListFiles.recursiveListFiles(new File(pathGMQL)).filter(f => regexBedMetaTCGA.findFirstIn(f.getName).isDefined).map(path => {
          val tables = new TCGATables().getListOfTables()
          fromDbToTsv.setTable(tables._1, tables._2, tables._3, tables._4, tables._5, tables._6, tables._7, tables._8)
          fromDbToTsv.run(path.getAbsolutePath, exportRegexTCGA)
        })
      }
      case _ => logger.error(s"Incorrect repository")
    }

    val t3: Long = System.nanoTime()

    logger.info(s"Total time for the write info in TSV file ${getTotalTimeFormatted(t2, t3)}")
    logger.info(s"Total file analized ${Statistics.tsvFile}")
    logger.info(s"File correctly exported ${Statistics.correctExportedFile}")
    logger.info(s"File exported with Error ${Statistics.errorExportedFile}")

  }

  def analizeFileEncode(path: String, pathXML: String): Unit = {
    val t0: Long = System.nanoTime()
    Statistics.fileNumber += 1
    logger.info(s"Start to read $path")
    try {
      val lines = Source.fromFile(path).getLines.toArray
      //var states = collection.mutable.Map[String, String]()
      filePath = path
      val encodesTableId = new EncodeTableId
      val bioSampleList = new BioSampleList(lines,encodesTableId)
      val replicateList = new ReplicateList(lines,bioSampleList)
      encodesTableId.bioSampleQuantity(bioSampleList.BiosampleList.length)
      encodesTableId.setQuantityTechReplicate(replicateList.UuidList.length)
      encodesTableId.techReplicateArray(replicateList.BiologicalReplicateNumberList.toArray)
      var tables = new EncodeTables(encodesTableId)
      tables.filePath_:(path)
      tables.setPathOnTables()

      var states = createMapper(lines)

      var status: String = "archived"
      var analizeFileBool: Boolean = false
      if (states.contains("file__status")) {
        if (states("file__status").equals("released")) {
          status = "released"
          analizeFileBool = true
        }
      }
      if (analizeFileBool) {
        Statistics.released += 1
        logger.info(s"File status released, start populate table")
        val xml = new XMLReaderEncode(pathXML, replicateList, bioSampleList, states)
        val operationsList = xml.operationsList
        val t1: Long = System.nanoTime()
        Statistics.incrementExtractTime(t1-t0)
        operationsList.map(x => {
          try {
            populateTable(x, tables.selectTableByName(x.head), states.toMap)

          } catch {
            case e: NoSuchElementException => {
              tables.nextPosition(x.head, x(2), x(3)); logger.warn(s"SourceKey doesn't find for $x")
            }
          }})
        val t2: Long = System.nanoTime()
        Statistics.incrementTrasformTime((t2-t1))
        tables.insertTables()
        val t3: Long = System.nanoTime()
        Statistics.incrementLoadTime((t3-t2))
      }
      else {
        Statistics.archived += 1
        logger.info(s"File status $status, go to next file")
      }
    } catch {
      case aioobe: ArrayIndexOutOfBoundsException => {
        logger.error(s"ArrayIndexOutOfBoundsException file with path ${path}")
        Statistics.indexOutOfBoundsException += 1
      }
      /*case e: Exception => {
        logger.error(s"Unknown File input Exception file with path ${path}")
        Statistics.anotherInputException += 1
      }*/
    }
  }

  def analizeFileTCGA(path: String, pathXML: String): Unit = {
    val t0: Long = System.nanoTime()
    Statistics.fileNumber += 1
    logger.info(s"Start to read $path")
    try{
      val lines = Source.fromFile(path).getLines.toArray
      var states = collection.mutable.Map[String, String]()

      filePath = path
      var tables = new TCGATables

      for (l <- lines) {
        val first = l.split("\t", 2)
        if(first.size == 2)
          states += (first(0) -> first(1))
        else {
          logger.warn(s"Malformation in line ${first(0)}")
          Statistics.malformedInput += 1
        }
      }
      Statistics.released += 1
      logger.info(s"File status released, start populate table")
      val xml = new XMLReaderTCGA(pathXML)
      val operationsList = xml.operationsList
      val t1: Long = System.nanoTime()
      Statistics.incrementExtractTime(t1-t0)
      operationsList.map(x =>
        try {
          populateTable(x, tables.selectTableByName(x.head), states.toMap)

        } catch {
          case e: NoSuchElementException => logger.warn(s"SourceKey does't find for $x")
        })
      val t2: Long = System.nanoTime()
      Statistics.incrementTrasformTime((t2-t1))
      tables.insertTables()
      val t3: Long = System.nanoTime()
      Statistics.incrementLoadTime((t3-t2))
    } catch {
      case aioobe: ArrayIndexOutOfBoundsException => {
        logger.error(s"ArrayIndexOutOfBoundsException file with path ${path}")
        Statistics.indexOutOfBoundsException += 1
      }
      case e: Exception => {
        logger.error(s"Unknown File input Exception file with path ${path}")
        Statistics.anotherInputException += 1
      }
    }
  }

  def populateTable(list: List[String], table: Table, states: Map[String,String]): Unit = {
    //val insertMethod = InsertMethod.selectInsertionMethod(list(1),list(2),list(3), list(4), list(5), list(6), list(7))
    val insertMethod = InsertMethodNew.selectInsertionMethod(list(1),list(2),list(3), list(4), list(5), list(6), list(7))

    if(list(3).contains("MANUALLY"))
      table.setParameter(list(1), list(2), insertMethod)
    else
      table.setParameter(states(list(1)), list(2), insertMethod)
  }

  def defineFileAppenderSetting(logName: String): Unit = {
    val fa2 = new FileAppender()
    fa2.setName("FileLogger")
    fa2.setFile(logName)
    fa2.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"))
    fa2.setThreshold(Level.DEBUG)
    fa2.setAppend(true)
    fa2.activateOptions()
    Logger.getLogger("it.polimi.genomics.importer").addAppender(fa2)
  }

  def createMapper(lines: Array[String]): collection.mutable.Map[String, String] = {
    var states = collection.mutable.Map[String, String]()
    for (l <- lines) {
      val first = l.split("\t", 2)
      if(first.size == 2) {
        if (states.contains(first(0)))
          states += (first(0) -> states(first(0)).concat(conf.getString("import.multiple_value_concatenation") + first(1)))
        else
          states += (first(0) -> first(1))
      }
      else {
        logger.warn(s"Malformation in line ${first(0)}")
        Statistics.malformedInput += 1
      }
    }
    states
  }

}

