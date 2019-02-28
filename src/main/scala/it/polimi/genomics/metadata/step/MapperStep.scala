package it.polimi.genomics.metadata.step

import java.io._

import com.typesafe.config.ConfigFactory
import it.polimi.genomics.metadata.Program.getTotalTimeFormatted
import it.polimi.genomics.metadata.database.FileDatabase
import it.polimi.genomics.metadata.step.utils.{DirectoryNamingUtil, ParameterUtil, SchemaValidator}
import it.polimi.genomics.metadata.mapper.{Predefined, REP, Table}
import it.polimi.genomics.metadata.mapper.Encode.{EncodeTableId, EncodeTables}
import it.polimi.genomics.metadata.mapper.Encode.Utils.{BioSampleList, ReplicateList}
import it.polimi.genomics.metadata.mapper.REP.{REPTableId, REPTables}
import it.polimi.genomics.metadata.mapper.RemoteDatabase.DbHandler
import it.polimi.genomics.metadata.mapper.TCGA.TCGATables
import it.polimi.genomics.metadata.mapper.Utils._
import it.polimi.genomics.metadata.step.xml.{Dataset, Source}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable


object MapperStep extends Step {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

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

  val conf = ConfigFactory.load()


  override def execute(source: Source, parallelExecution: Boolean): Unit = {
    if (source.mapperEnabled) {

      logger.info("Starting mapper for: " + source.outputFolder)
      val sourceId = FileDatabase.sourceId(source.name)

      //counters
      var modifiedRegionFilesSource = 0
      var modifiedMetadataFilesSource = 0
      var wrongSchemaFilesSource = 0
      //integration process for each dataset contained in the source.
      val integrateThreads = source.datasets.map((dataset: Dataset) => {
        new Thread {
          override def run(): Unit = {
            val source_name = dataset.fullDatasetOutputFolder

            val originalFileName: String =
              if (file._3 == 1) file._2
              else file._2.replaceFirst("\\.", "_" + file._3 + ".")

            val datasetOutputFolder = dataset.fullDatasetOutputFolder
            val transformations2Folder = datasetOutputFolder + File.separator + DirectoryNamingUtil.transformFolderName

            val inputFilePath = transformations2Folder + File.separator + originalFileName


            val cleanedFilesPath = ParameterUtil.getParameter(dataset, "rule_base")
            val mappingsDefinition = ParameterUtil.getParameter(dataset, "mappings")
            importMode(args(1), args(2), args(3))


            //arg 1 rep
            // arg 2 / Users / abernasconi / Documents / gitProjects / GMQL -Importer / Example / examples_meta / rep
            // arg 3 / Users / abernasconi / Documents / gitProjects / GMQL -Importer / Example / xml / settingsREP.xml
            //            val cleanedDirectory = DirectoryNamingUtil.getCleanedDirectory(dataset)
            //            val mappingsOpt = ParameterUtil.getParameter(dataset, "mappings")
            //            val databaseConfig = ParameterUtil.gcmConfigFile
            //
            //
            //            //call mapper(import) function with cleaning directory. I will send also the dataset name
            //
            //
            //            val rulePathOpt = dataset.parameters.filter(_._1 == "region_sorting").headOption.map(_._2)
            //            if (dataset.transformEnabled) {
            //              val ruleBasePathOpt = rulePathOpt.map(new RuleBase(_))
            //
            //
            //              MapperMain.importMode()
            //
            //              val t0Dataset: Long = System.nanoTime()
            //              var modifiedRegionFilesDataset = 0
            //              var modifiedMetadataFilesDataset = 0
            //              var wrongSchemaFilesDataset = 0
            //              var totalTransformedFiles = 0
            //              val datasetId = FileDatabase.datasetId(sourceId, dataset.name)
            //
            //              val datasetOutputFolder = dataset.fullDatasetOutputFolder
            //              //              val downloadsFolder = datasetOutputFolder + File.separator + "Downloads"
            //              val transformations2Folder = datasetOutputFolder + File.separator + "Transformations"
            //              val cleanerFolder = datasetOutputFolder + File.separator + "Cleaned"
            //
            //
            //              val folder = new File(cleanerFolder)
            //              if (folder.exists()) {
            //                Transformer.deleteFolder(folder)
            //              }
            //
            //
            //              logger.info("Starting download for: " + dataset.name)
            //              // puts the schema into the transformations folder.
            //              if (schemaFinder.downloadSchema(source.rootOutputFolder, dataset, cleanerFolder, source))
            //                logger.debug("Schema downloaded for: " + dataset.name)
            //              else
            //                logger.warn("Schema not found for: " + dataset.name)
            //
            //
            //              if (!folder.exists()) {
            //                folder.mkdirs()
            //                logger.debug("Folder created: " + folder)
            //              }
            //              logger.info("Cleaner for dataset: " + dataset.name)
            //
            //              FileDatabase.delete(datasetId, STAGE.CLEAN)
            //              //id, filename, copy number.
            //              var filesToTransform = 0
            //
            //
            //              FileDatabase.getFilesToProcess(datasetId, STAGE.TRANSFORM).foreach { file =>
            //                val originalFileName: String =
            //                  if (file._3 == 1) file._2
            //                  else file._2.replaceFirst("\\.", "_" + file._3 + ".")
            //
            //                val inputFilePath = transformations2Folder + File.separator + originalFileName
            //                val outFilePath = cleanerFolder + File.separator + originalFileName
            //
            //                val fileId = FileDatabase.fileId(datasetId, inputFilePath, STAGE.CLEAN, originalFileName)
            //
            //                if (inputFilePath.endsWith(".meta") && ruleBasePathOpt.isDefined) {
            //                  ruleBasePathOpt.get.applyRBFile(inputFilePath, outFilePath)
            //                } else {
            //                  createSymbolicLink(inputFilePath, outFilePath)
            //                }
            //
            //                if (true)
            //                  FileDatabase.markAsUpdated(fileId, new File(outFilePath).length.toString)
            //                else
            //                  FileDatabase.markAsFailed(fileId)
            //
            //              }
            //              FileDatabase.markAsOutdated(datasetId, STAGE.CLEAN)
            //
            //              FileDatabase.runDatasetTransformAppend(datasetId, dataset, filesToTransform, totalTransformedFiles)
            //              modifiedMetadataFilesSource = modifiedMetadataFilesSource + modifiedMetadataFilesDataset
            //              modifiedRegionFilesSource = modifiedRegionFilesSource + modifiedRegionFilesDataset
            //              wrongSchemaFilesSource = wrongSchemaFilesSource + wrongSchemaFilesDataset
            //              //              logger.info(modifiedRegionFilesDataset + " region data files modified in dataset: " + dataset.name)
            //              //              logger.info(modifiedMetadataFilesDataset + " metadata files modified in dataset: " + dataset.name)
            //              //              logger.info(wrongSchemaFilesDataset + " region data files do not respect the schema in dataset: " + dataset.name)
            //              //              val t1Dataset = System.nanoTime()
            //              //              logger.info(s"Total time for transformation dataset ${dataset.name}: ${Transformer.getTotalTimeFormatted(t0Dataset, t1Dataset)}")
            //
            //              DatasetNameUtil.saveDatasetName(dataset)
            //
            //            }
            //          }
            //        }
            //      })
            //      if (parallelExecution) {
            //        integrateThreads.foreach(_.start())
            //        integrateThreads.foreach(_.join())
            //      }
            //      else {
            //        for (thread <- integrateThreads) {
            //          thread.start()
            //          thread.join()
            //        }
            //      }
            //      logger.info(modifiedRegionFilesSource + " region data files modified in source: " + source.name)
            //      logger.info(modifiedMetadataFilesSource + " metadata files modified in source: " + source.name)
            //      logger.info(wrongSchemaFilesSource + " region data files do not respect the schema in source: " + source.name)
            //      logger.info(s"Source ${source.name} transformation finished")
            //    }
            //  }

          }
        }

      })
      if (parallelExecution) {
        integrateThreads.foreach(_.start())
        integrateThreads.foreach(_.join())
      }
      else {
        for (thread <- integrateThreads) {
          thread.start()
          thread.join()
        }
      }
      logger.info(s"Source ${source.name} mapping finished")
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
    for (l <- linesFromSet.toList) {
      val pair = l.split("\t", 2)
      if (pair.length == 2) {

        if (pair(0).contains("age_weeks") && pair(1) == "unknown") {
          linesFromSet -= l
          logger.warn(s"Ignoring unknown age_weeks for roadmap epigenomics")
        }
        if (pair(0) == "epi__donor_id") {
          linesFromSet -= l
          for (b <- bioNumbers) {
            linesFromSet += "epi__donor_id__" + b + "\t" + pair(1)
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
            linesFromSet += "epi__sample_alias__" + b + "\t" + pair(1)
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