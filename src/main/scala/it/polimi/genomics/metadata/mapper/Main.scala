/*
package it.polimi.genomics.metadata.mapper



import it.polimi.genomics.metadata.mapper.Utils._
import java.io._

import java.util
import scala.collection.JavaConverters._

import com.typesafe.config.ConfigFactory
import it.polimi.genomics.metadata.mapper.RemoteDatabase.DbHandler
import it.polimi.genomics.metadata.Program.getTotalTimeFormatted
import org.apache.log4j._
import it.polimi.genomics.metadata.mapper.Encode.Utils.{BioSampleList, ReplicateList}
import it.polimi.genomics.metadata.mapper.Encode.{EncodeTableId, EncodeTables}
import it.polimi.genomics.metadata.mapper.REP.{REPTableId, REPTables}
import it.polimi.genomics.metadata.mapper.TCGA.TCGATables
import it.polimi.genomics.metadata.step.utils.SchemaValidator
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.openqa.selenium.{By, WebElement}
import org.openqa.selenium.htmlunit.HtmlUnitDriver

import scala.collection.mutable
import scala.io.Source

import it.polimi.genomics.metadata.step.utils.DirectoryNamingUtil

object main {
  val logger: Logger = Logger.getLogger(this.getClass)
  private val regexMeta = ".*.meta".r
  private val regexRaoTads = ".*_rao.*".r
  private val regexCombTads = ".*_(rep1|rep2|combined).*".r

  object SourceString extends Enumeration {
    type SourceString = Value

    val encodeString = Value("ENCODE")
    val tcgaString = Value("TCGA")
    val roadmapString = Value("REP")
    val annotationString = Value("ANN")
    val tadsraoString = Value("TADS_RAO")
    val tadscombString = Value("TADS_COMB")
    val cistromeString = Value("CISTROME")

    def isSourceString(s: String) = values.exists(_.toString == s)

    override def toString: String = {
      var toPrint: String = ""
      for (s <- values) {
        toPrint = s + ", " + toPrint
      }
      toPrint.substring(0, toPrint.size - 2)
    }

  }


  var filePath: String = _

  val conf = ConfigFactory.load()


  def main(args: Array[String]): Unit = {

    val PATTERN = "%d [%p|%c|%C{1}] %m%n"
    val consoleINFO = new ConsoleAppender()

    //configure the appender
    consoleINFO.setLayout(new PatternLayout(PATTERN))
    consoleINFO.setThreshold(Level.INFO)
    consoleINFO.activateOptions()
    Logger.getLogger("it.polimi.genomics").addAppender(consoleINFO)

    //configure the appender
    val consoleWARN = new ConsoleAppender()
    consoleWARN.setLayout(new PatternLayout(PATTERN))
    consoleWARN.setThreshold(Level.WARN)
    consoleWARN.activateOptions()
    Logger.getLogger("slick").addAppender(consoleWARN)

    //set connection and create the tables if not exists


    if (args.length == 0) {
      logger.warn(s"No arguments specified")
      return
    }
    if (args.length < 4 || args.length > 4) {
      logger.error(s"Incorrect number of arguments:")
      logger.info("GMQLImporter help; in order to import Data from File:\n"
        + "\t Run with configuration_xml_path, file_folder, repository_ref and IMPORT as arguments\n"
      )
      logger.info("GMQLImporter help; in order to export Data from Database to file:\n"
        + "\t Run with file_folder, repository_ref and EXPORT as arguments\n"
      )
      return
    }
    if (args(0).toUpperCase != "IMPORT" && args(0).toUpperCase != "EXPORT") {
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
      if (!SourceString.isSourceString(args(1).toUpperCase)) { //checks if source inserted as argument 1 is one of the supported ones
        logger.error(s"The source specified as second argument is currently not supported")
        logger.info("Please select among the following: " + SourceString.toString)
        return
      }

      val logName = "IMPORT_" + args(1).toUpperCase + "_" + DateTime.now.toString(DateTimeFormat.forPattern("yyyy_MM_dd HH:mm:ss.SSS Z")) + ".log"

      defineFileAppenderSetting(logName)

      importMode(args(1), args(2), args(3))
    } else {

      logger.info("EXPORT MODE IS DISABLED. PLEASE RUN FLATTENER using settings.xml")

    }
  }


  def importMode(repositoryRef: String, pathGMQLIn: String, pathXML: String): Unit = {
    //DbHandler.setDatabase()

    val datasetFileName = pathGMQLIn + File.separator + "dataset_name.txt"
    val datasetName = Source.fromFile(datasetFileName).mkString.trim
    Predefined.map += "dataset_name" -> datasetName

    val pathGMQL = pathGMQLIn + File.separator + DirectoryNamingUtil.cleanFolderName + File.separator


    val schemaUrl = "https://raw.githubusercontent.com/DEIB-GECO/Metadata-Manager/master/Example/xml/setting.xsd"
    if (SchemaValidator.validate(pathXML, schemaUrl)) {
      logger.info("Xml file is valid for the schema")
      DbHandler.setDatabase
      DbHandler.setDWViews
      DbHandler.setFlattenMaterialized
      logger.info("Database has been set")

      val t0: Long = System.nanoTime()
      repositoryRef.toUpperCase() match {
        case "ENCODE" => ListFiles.recursiveListFiles(new File(pathGMQL)).filter(f => regexMeta.findFirstIn(f.getName).isDefined).map(path => analyzeFileEncode(path.toString, pathXML))
        case "REP" => ListFiles.recursiveListFiles(new File(pathGMQL)).filter(f => regexMeta.findFirstIn(f.getName).isDefined).map(path => analyzeFileRep(path.toString, pathXML))
        case "TCGA" | "ANN" | "CISTROME" => ListFiles.recursiveListFiles(new File(pathGMQL)).filter(f => regexMeta.findFirstIn(f.getName).isDefined).map(path => analyzeFileRegular(path.toString, pathXML))
        case "TADS_RAO" => ListFiles.recursiveListFiles(new File(pathGMQL)).filter(f => regexRaoTads.findFirstIn(f.getName).isDefined).filter(f => regexMeta.findFirstIn(f.getName).isDefined).map(path => analyzeFileRegular(path.toString, pathXML))
        case "TADS_COMB" => ListFiles.recursiveListFiles(new File(pathGMQL)).filter(f => regexCombTads.findFirstIn(f.getName).isDefined).filter(f => regexMeta.findFirstIn(f.getName).isDefined).map(path => analyzeFileRep(path.toString, pathXML))
        case _ => logger.error(s"Incorrectly specified repository")
      }
      val t1 = System.nanoTime()
      logger.info(s"Total time to insert data in DB ${getTotalTimeFormatted(t0, t1)}")
      logger.info(s"Total analyzed files ${Statistics.fileNumber}")
      logger.info(s"Total considered files ${Statistics.released}")
      logger.info(s"Total discarded files ${Statistics.discarded}")
      //logger.info(s"Total not-considered files ${Statistics.archived}")
      //logger.info(s"Total file released but not inserted ${Statistics.releasedItemNotInserted}")
      logger.info(s"Total Item inserted ${Statistics.itemInserted}")
      logger.info(s"Total Item updated ${Statistics.itemUpdated}")
      logger.info(s"Constraints violated ${Statistics.constraintsViolated}")
      logger.info(s"Total malformation found  ${Statistics.malformedInput}")
      logger.info(s"ArrayIndexOutOfBoundsException file input  ${Statistics.indexOutOfBoundsException}")
      logger.info(s"UnknownInputException file input  ${Statistics.anotherInputException}")
      logger.info(s"Time to extract information from files  ${Statistics.getTimeFormatted(Statistics.extractTimeAcc)}")
      logger.info(s"Time to convert information into relational tuples  ${Statistics.getTimeFormatted(Statistics.transformTimeAcc)}")
      logger.info(s"Time to load information into tables  ${Statistics.getTimeFormatted(Statistics.loadTimeAcc)}")
      if (repositoryRef.toUpperCase.equals("ENCODE")) {
        logger.info(s"Total Donors inserted or updated ${Statistics.donorInsertedOrUpdated}")
        logger.info(s"Total Biosamples inserted or updated ${Statistics.biosampleInsertedOrUpdated}")
        logger.info(s"Total Replicates inserted or updated ${Statistics.replicateInsertedOrUpdated}")

      }

      DbHandler.refreshFlattenMaterialized

    }
    else
      logger.info("Xml file is NOT valid for the schema, please check it for next runs")
    DbHandler.closeDatabase()

  }


  def analyzeFileEncode(path: String, pathXML: String): Unit = {
    val t0: Long = System.nanoTime()
    Statistics.fileNumber += 1
    logger.info(s"Start reading $path")
    try {

      var lines = Source.fromFile(path).getLines.toList
      val str: Array[String] = path.split("/")
      val metadata_file_name: String = str.last //with extension for metadata file (.meta)
      val region_file_name: String = metadata_file_name.replace(".meta", "") //with extension for metadata file (.meta)
      val file_identifier: String = region_file_name.split("\\.").head //without extension
      val file_identifier_with_directory: String = file_identifier + "__" + str(str.size - 3)
      lines = lines ::: List("file_name\t" + region_file_name)
      lines = lines ::: List("file_identifier\t" + file_identifier)
      lines = lines ::: List("file_identifier_with_directory\t" + file_identifier_with_directory)

      val encodesTableId = new EncodeTableId
      // setting field _biosampleArray with the numbers of replicates present in the item
      val bioSampleList = new BioSampleList(lines.toArray, encodesTableId)
      val replicateList = new ReplicateList(lines.toArray, bioSampleList)

      //if replicateList is empty print a log and don't import file
      if (replicateList.UuidList.isEmpty) {
        Statistics.discarded += 1
        logger.warn(s"File ${path.split("/").last} is missing biological replicates. It is ignored...")
      }
      else {

        encodesTableId.bioSampleQuantity(bioSampleList.BiosampleList.length)
        encodesTableId.setQuantityTechReplicate(replicateList.UuidList.length)
        encodesTableId.techReplicateArray(replicateList.BiologicalReplicateNumberList.toArray)
        val tables = new EncodeTables(encodesTableId)
        tables.setFilePath(path)
        tables.setPathOnTables()

        val states: collection.mutable.Map[String, String] = createMapper(lines)

        Statistics.released += 1
        logger.info(s"Start populating tables for file ${path.split("/").last}")
        //reads XML file and substitutes X with number of replicates
        val xml = new XMLReaderEncode(pathXML, replicateList, bioSampleList, states)
        val operationsList = xml.operationsList
        val t1: Long = System.nanoTime()
        Statistics.incrementExtractTime(t1 - t0)
        operationsList.map(operations => {
          try {
            populateTable(operations, tables.selectTableByName(operations.head), states.toMap)
          } catch {
            case e: NoSuchElementException => {
              tables.nextPosition(operations.head, operations(2), operations(3));
              logger.warn(s"Source key: ${operations(1)} not found for Table: ${operations(0)}, Global key: ${operations(2)}")
            }
          }
        })
        val t2: Long = System.nanoTime()
        Statistics.incrementTrasformTime((t2 - t1))
        tables.insertTables(states, createPairs(lines))
        val t3: Long = System.nanoTime()
        Statistics.incrementLoadTime((t3 - t2))
      }

    } catch {
      case aioobe: ArrayIndexOutOfBoundsException => {
        logger.error(s"ArrayIndexOutOfBoundsException file with path ${path}")
        Statistics.indexOutOfBoundsException += 1
      }

    }
  }

  def analyzeFileRep(path: String, pathXML: String): Unit = {
    val t0: Long = System.nanoTime()
    Statistics.fileNumber += 1
    logger.info(s"Start reading $path")
    try {
      var old_lines = Source.fromFile(path).getLines.toList


      val str: Array[String] = path.split("/")
      val metadata_file_name: String = str.last //with extension for metadata file (.meta)
      val region_file_name: String = metadata_file_name.replace(".meta", "") //with extension for metadata file (.meta)
      val file_identifier: String = region_file_name.split("\\.").head //without extension
      val file_identifier_with_directory: String = file_identifier + "__" + str(str.size - 3)
      old_lines = old_lines ::: List("file_name\t" + region_file_name)
      old_lines = old_lines ::: List("file_identifier\t" + file_identifier)
      old_lines = old_lines ::: List("file_identifier_with_directory\t" + file_identifier_with_directory)


      val repTableId = new REPTableId

      //computes number of samples in the file
      val bioSampleList = new REP.Utils.BioSampleList(old_lines.toArray, repTableId)

      //in files with simple epi__donor_id, it adds the keys epi__donor_id__X for each biosample present (same for other attributes)
      val lines: Array[String] = enrichLinesREP(old_lines.toArray, bioSampleList, path)


      //prepares ficticious replicate tuples (id=biosample_id, bio/tech replicate number = 1)
      val replicateList = new REP.Utils.ReplicateList(lines, bioSampleList)

      repTableId.bioSampleQuantity(bioSampleList.BiosampleList.length)
      repTableId.setQuantityTechReplicate(replicateList.UuidList.length)
      repTableId.techReplicateArray(replicateList.BiologicalReplicateNumberList.toArray)
      val tables = new REPTables(repTableId)
      tables.setFilePath(path)
      tables.setPathOnTables()

      //key,value pairs (multiple values are concatenated with comma)
      val states: mutable.Map[String, String] = createMapper(lines.toList)

      Statistics.released += 1
      logger.info(s"File status released, start populating table")
      val xml = new XMLReaderREP(pathXML, replicateList, bioSampleList, states)
      val operationsList: List[List[String]] = xml.operationsList
      val t1: Long = System.nanoTime()
      Statistics.incrementExtractTime(t1 - t0)
      operationsList.foreach { (operations: List[String]) =>
        try {
          populateTable(operations, tables.selectTableByName(operations.head), states.toMap)
        } catch {
          case e: NoSuchElementException => {
            tables.nextPosition(operations.head, operations(2), operations(3));
            logger.warn(s"Source key: ${operations(2)} not found for Table: ${operations(1)}, Global key: ${operations(3)}")
          }
        }
      }
      val t2: Long = System.nanoTime()
      Statistics.incrementTrasformTime((t2 - t1))
      tables.insertTables(states, createPairs(lines.toList))
      val t3: Long = System.nanoTime()
      Statistics.incrementLoadTime((t3 - t2))

    } catch {
      case aioobe: ArrayIndexOutOfBoundsException => {
        logger.error(s"ArrayIndexOutOfBoundsException file with path ${path}")
        Statistics.indexOutOfBoundsException += 1
      }
    }
  }

  def analyzeFileRegular(path: String, pathXML: String): Unit = {
    val t0: Long = System.nanoTime()
    Statistics.fileNumber += 1
    logger.info(s"Start reading $path")
    try {

      var lines = Source.fromFile(path).getLines.toList
      val str: Array[String] = path.split("/")
      val metadata_file_name: String = str.last //with extension for metadata file (.meta)
      val region_file_name: String = metadata_file_name.replace(".meta", "") //with extension for metadata file (.meta)
      val file_identifier: String = region_file_name.split("\\.").head //without extension
      val file_identifier_with_directory: String = file_identifier + "__" + str(str.size - 3)
      lines = lines ::: List("file_name\t" + region_file_name)
      lines = lines ::: List("file_identifier\t" + file_identifier)
      lines = lines ::: List("file_identifier_with_directory\t" + file_identifier_with_directory)

      val path_region_file = path.replaceAll(".meta$", "")
      val region_file = new File(path_region_file)
      val region_file_size = region_file.length
      if (region_file_size != 0)
        lines = lines ::: List("region_file_size\t" + region_file.length)

      val tables = new TCGATables

      //key,value pairs (multiple values are concatenated with comma)
      val states: mutable.Map[String, String] = createMapper(lines)

      Statistics.released += 1
      logger.info(s"File status released, start populating table")
      val xml = new XMLReaderTCGA(pathXML)
      val operationsList = xml.operationsList
      val t1: Long = System.nanoTime()
      Statistics.incrementExtractTime(t1 - t0)
      operationsList.map(operations =>
        try {
          populateTable(operations, tables.selectTableByName(operations.head), states.toMap)

        } catch {
          case e: NoSuchElementException =>
            //logger.warn(s"SourceKey not found for $x")
            logger.warn(s"Source key: ${operations(1)} not found for Table: ${operations(0)}, Global key: ${operations(2)}")
        })
      val t2: Long = System.nanoTime()
      Statistics.incrementTrasformTime((t2 - t1))
      tables.insertTables(states, createPairs(lines))
      val t3: Long = System.nanoTime()
      Statistics.incrementLoadTime((t3 - t2))
    } catch {
      case aioobe: ArrayIndexOutOfBoundsException => {
        logger.error(s"ArrayIndexOutOfBoundsException file with path ${path}")
        Statistics.indexOutOfBoundsException += 1
      }
      case e: Exception => {
        logger.error(s"Unknown File input Exception file with path ${path}")
        e.printStackTrace()
        Statistics.anotherInputException += 1
      }
    }
  }

  def populateTable(list: List[String], table: Table, states: Map[String, String]): Unit = {

    val insertMethod = InsertMethod.selectInsertionMethod(list(1), list(2), list(3), list(4), list(5), list(6), list(7))

    if (list(3).contains("MANUAL") || list(3).contains("PREDEFINED"))
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
    Logger.getLogger("it.polimi.genomics").addAppender(fa2)
  }

  def createMapper(lines: List[String]): collection.mutable.Map[String, String] = {
    var temp_states = collection.mutable.Map[String, mutable.SortedSet[String]]()
    var states = collection.mutable.Map[String, String]()

    for (l <- lines) {
      val first = l.split("\t", 2)
      if (first.size == 2) {
        if (temp_states.contains(first(0))) {
          val newset: mutable.SortedSet[String] = temp_states(first(0))
          newset += first(1)
          temp_states += (first(0) -> newset)
        }
        else {
          val newset2 = scala.collection.mutable.SortedSet[String]()
          newset2 += first(1)
          temp_states += (first(0) -> newset2)
        }
      }
      else {
        logger.warn(s"Malformation in line ${first(0)}")
        Statistics.malformedInput += 1
      }

      for (z <- temp_states.keys) {
        val concat_char = conf.getString("import.multiple_value_concatenation")
        states += (z -> temp_states(z).mkString(concat_char))
      }

    }
    states
  }

  def enrichLinesREP(lines: Array[String], bioSampleList: REP.Utils.BioSampleList, path: String): Array[String] = {
    val bioNumbers = 1 to bioSampleList.BiosampleList.length toList
    var linesFromSet = scala.collection.mutable.Set(lines: _*) //transform array into set
    //RETRIEVE EPIGENOME NAME
    var epigenome_prefix = "Edef__"

    for (l <- linesFromSet.toList) {
      val pair = l.split("\t", 2)
      if (pair.length == 2) {

        if (pair(0).contains("epi__epigenome_id")) {
          epigenome_prefix = pair(1) + "__"
        }
      }
      else {
        logger.warn(s"Malformation in line ${pair(0)}. Not able to retrieve epigenome name")
        Statistics.malformedInput += 1
      }
    }

    for (l <- linesFromSet.toList) {
      val pair = l.split("\t", 2)
      if (pair.length == 2) {

        if (pair(0).contains("age_weeks") && pair(1) == "unknown") {
          linesFromSet -= l
          logger.warn(s"Ignoring unknown age_weeks for roadmap epigenomics")
        }
        if (pair(0).startsWith("epi__donor_id__") || pair(0).startsWith("epi__sample_alias__")) {
          linesFromSet -= l
          for (b <- bioNumbers) {
            linesFromSet += pair(0) + "\t" + epigenome_prefix + pair(1) //added prefix epigenome
          }
        }
        if (pair(0) == "epi__donor_id") {
          linesFromSet -= l
          for (b <- bioNumbers) {
            linesFromSet += "epi__donor_id__" + b + "\t" + epigenome_prefix + pair(1) //added prefix epigenome
          }
        }
        if (pair(0) == "epi__age_weeks") {
          linesFromSet -= l
          for (b <- bioNumbers) {
            linesFromSet += "epi__age_weeks__" + b + "\t" + pair(1)
          }
        }
        if (pair(0) == "epi__ethnicity") {
          linesFromSet -= l
          for (b <- bioNumbers) {
            linesFromSet += "epi__ethnicity__" + b + "\t" + pair(1)
          }
        }
        if (pair(0) == "epi__sample_alias") {
          linesFromSet -= l
          for (b <- bioNumbers) {
            linesFromSet += "epi__sample_alias__" + b + "\t" + epigenome_prefix + pair(1)
          }
        }
      }
      else {
        logger.warn(s"Malformation in line ${pair(0)}")
        Statistics.malformedInput += 1
      }
    }


    val manCurId = linesFromSet.toList.map { l =>
      val pair = l.split("\t", 2)
      if (pair.length == 2)
        if (pair(0).startsWith("manually_curated__file_id"))
          Some(pair(1))
        else
          None
      else
        None
    }.flatten.headOption.getOrElse("Error during EnrichLinesREP method")

    val hasDonorId = linesFromSet.toList.map { l =>
      val pair = l.split("\t", 2)
      if (pair.length == 2)
        if (pair(0).startsWith("epi__donor_id"))
          true
        else
          false
      else
        false
    }.reduce(_ || _)

    val hasSampleId = linesFromSet.toList.map { l =>
      val pair = l.split("\t", 2)
      if (pair.length == 2)
        if (pair(0).startsWith("epi__sample_alias"))
          true
        else
          false
      else
        false
    }.reduce(_ || _)


    if (!hasDonorId)
      linesFromSet += "epi__donor_id__1" + "\t" + manCurId

    if (!hasSampleId)
      linesFromSet += "epi__sample_alias__1" + "\t" + manCurId

    linesFromSet.toArray
  }

  def createPairs(lines: List[String]): List[(String, String)] = {
    var pairs = List[(String, String)]()
    for (l <- lines) {
      val first = l.split("\t", 2)
      if (first.size == 2) {
        pairs = pairs ::: List((first(0).replaceAll("__[0-9]+__", "__"), first(1)))
      }
      else {
        logger.warn(s"Malformation in line ${
          first(0)
        }")
        Statistics.malformedInput += 1
      }
    }
    pairs
  }

}

 */