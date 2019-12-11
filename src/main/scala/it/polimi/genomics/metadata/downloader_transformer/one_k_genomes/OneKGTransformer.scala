package it.polimi.genomics.metadata.downloader_transformer.one_k_genomes

import java.io.{BufferedReader, File}
import java.util

import it.polimi.genomics.metadata.database.FileDatabase
import it.polimi.genomics.metadata.downloader_transformer.Transformer
import it.polimi.genomics.metadata.downloader_transformer.default.utils.Unzipper
import it.polimi.genomics.metadata.step.xml.{Dataset, Source}
import it.polimi.genomics.metadata.util.{FileUtil, ManyToFewMap, PatternMatch}
import org.apache.commons.cli.MissingArgumentException
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.{immutable, mutable}

/**
 * Created by Tom on ott, 2019
 *
 * Transformer for source 1000Genomes.
 * Since the transformation of VCF files (region data) is quite complex, it is delegated to VCFAdapter, while metadata
 * files are transformed directly here.
 */
class OneKGTransformer extends Transformer {
  import OneKGTransformer._

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  // remember extracted archives & early transformed VCFs
  protected val extractedArchives: mutable.Map[String, String] = mutable.HashMap.empty[String, String]
  protected val transformedVCFs: mutable.Set[String] = mutable.HashSet.empty[String]
  // metadata (depend on dataset)
  protected var dataset: Dataset = _
  protected var datasetParameters:mutable.LinkedHashMap[String, String] = mutable.LinkedHashMap("treeFileName" -> "", "seqIndexMetaName" -> "", "populationMetaName" -> "",
    "individualDetailsMetaName" -> "", "schemaURL" -> "", "assembly" -> "", "manually_curated__pipeline" -> "",
    "samplesOriginMetaName" -> "")
  protected var manually_curated__data_url: List[String] = _
  protected var sequencingMetadata: ManyToFewMap[String, List[String]] = _
  protected var populationMetadata: Map[String, List[String]] = _
  protected var individualsMetadata: ManyToFewMap[String, List[String]] = _
  protected var samplesOriginMetadata: ManyToFewMap[String, String] = _
  // formatting options (depend on dataset)
  protected var mutationPrinter: MutationPrinterTrait = _


  /**
   * From a downloaded file, writes the given candidate file as a metadata or region file.
   *
   * For 1000Genomes, when the target filename is a region file, the input can only be one of the compressed VCFs. To
   * improve the execution time, the input VCFs are processed "once-and-for-all" converting all the mutations
   * contained in it for all the samples and not just for the target sample received as argument "filename".
   *
   * @param source           representation of the source XMl element
   * @param originPath       path for the  "Downloads" folder without trailing file separator and system dependent slash convention
   * @param destinationPath  path for the "Transformations" folder without trailing file separator and system dependent slash convention
   * @param originalFilename name of a compressed VCF file or metadata file with copy number
   * @param filename         name of the region data or metadata file to create and populate with the attributes from originFilename
   * @return true if the transformation produced a file, false otherwise (the candidate filename will be marked as failed)
   */
  override def transform(source: Source, originPath: String, destinationPath: String, originalFilename: String, filename: String): Boolean = {
    val sourceFileName = removeCopyNumber(originalFilename)
    val targetFileName = filename
    val sourceFilePath = originPath+File.separator+sourceFileName
    val targetFilePath = destinationPath+File.separator+targetFileName
    val transformationsDirPath = destinationPath+File.separator

    if (filename.endsWith(".gdm"))
      transformRegion(sourceFilePath, transformationsDirPath)
    else
      transformMetadata(targetFileName, targetFilePath)
  }

  protected def transformRegion(sourceFilePath:String, transformationsDirPath:String): Boolean ={
    // append region data
    extractArchive(sourceFilePath, removeExtension(sourceFilePath)) match {
      case None => false
      case Some(_VCFPath) =>
        // the source file is completely transformed generating the target region files for all the samples available in the source file
        if(!transformedVCFs.contains(_VCFPath)){
          new VCFAdapter(_VCFPath, mutationPrinter)
            .appendAllMutationsBySample(transformationsDirPath)
          transformedVCFs.add(_VCFPath)
        }
        true
    }
  }

  protected def transformMetadata(targetFileName:String, targetFilePath:String): Boolean ={
    val sampleName = removeExtension(removeExtension(targetFileName))
    val metadataContainer = new util.TreeMap[String, mutable.Set[String]]

    // study, center mame, sample_id (!=sample name), population, experiment, instrument, insert size, library, analysis group
    val thisSampleSequencingData = sequencingMetadata.getFirstOf(sampleName).get
    addToMetadataContainer(metadataContainer,
      List("study_id", "study_name", "center_name", "sample_id", "population", "experiment_id", "instrument_platform",
        "instrument_model", "insert_size", "library_layout", "analysis_group"),
      thisSampleSequencingData)

    // super-population, population's dna from blood
    val population:String = thisSampleSequencingData(4)
    val thisPopulationData = populationMetadata(population)
    val populationHasDNAFromBlood = thisPopulationData(1).toLowerCase == "yes"
    addToMetadataContainer(metadataContainer, "super_population", thisPopulationData.head)
    addToMetadataContainer(metadataContainer, "DNA_from_blood", populationHasDNAFromBlood.toString.toUpperCase)

    // family id, gender
    addToMetadataContainer(metadataContainer, List("family_id", "gender"), individualsMetadata.getFirstOf(sampleName).get)

    // manually curated metadata
    addToMetadataContainer(metadataContainer, "manually_curated__assembly", datasetParameters("assembly"))
    samplesOriginMetadata.getFirstOf(sampleName).get.toLowerCase match {
      case "blood" =>
        addToMetadataContainer(metadataContainer, "manually_curated__biosample_type", "tissue")
        addToMetadataContainer(metadataContainer, "manually_curated__biosample_tissue", "Blood")
      case "lcl" =>
        addToMetadataContainer(metadataContainer, "manually_curated__biosample_type", "cell line")
        addToMetadataContainer(metadataContainer, "manually_curated__cell_line", "lymphoblastoid cell line")
        addToMetadataContainer(metadataContainer, "manually_curated__cell_line_type", "B")
      case "" =>
        if(populationHasDNAFromBlood) {
          addToMetadataContainer(metadataContainer, "manually_curated__biosample_type", "tissue")
          addToMetadataContainer(metadataContainer, "manually_curated__biosample_tissue", "Blood")
        }
      case uncoveredCase =>
        logger.error("SAMPLE ORIGIN WITH VALUE "+uncoveredCase+" NOT PARSED")
    }
    addToMetadataContainer(metadataContainer, "manually_curated__data_type", "variant calling")
    addToMetadataContainer(metadataContainer, List.fill(getURLsOfOriginRegionData().size)("manually_curated__data_url"), getURLsOfOriginRegionData())
    addToMetadataContainer(metadataContainer, "manually_curated__feature", "variants")
    addToMetadataContainer(metadataContainer, "manually_curated__file_format", "bed")
    addToMetadataContainer(metadataContainer, "manually_curated__file_name", sampleName+".gdm")
    addToMetadataContainer(metadataContainer, "manually_curated__is_healthy", "TRUE")
    addToMetadataContainer(metadataContainer, "manually_curated__pipeline", datasetParameters("manually_curated__pipeline"))
    addToMetadataContainer(metadataContainer, "manually_curated__project_source", "1000 Genomes")
    addToMetadataContainer(metadataContainer, "manually_curated__source_page", DatasetInfo.parseDirectoryFromURL(getURLsOfOriginRegionData().head))
    addToMetadataContainer(metadataContainer, "manually_curated__species", "Homo Sapiens")

    writeMetadataContainer(metadataContainer, targetFilePath)
    true
  }

  /**
   * This is the 1st callback from the parent class Transformer. To each successfully downloaded file, it assigns a
   * list of filenames, region and/or metadata files, that the transformation stage is going to produce once completed.
   * The produced filenames must correspond to sample names with extension .gdm or .gdm.meta and the same filename
   * can be returned even multiple times without any consequences.
   * This method will be called once for every updated file in the origin dataset, but at the end of all the callbacks,
   * only the output filenames having corresponding region and metadata files will be kept, stored in the database
   * and issued as arguments to the method transform. Additionally, only the input filenames which produced at least
   * one sample or region filename will be issued as arguments to the method transform together with the produced list.
   *
   * Metadata files produce samplename.gdm.meta files, while region files produce samplename.gdm files.
   *
   * In the case of 1000Genomes, only the metadata files containing sequencing or individual details information contain
   * also the sample names, so the list of metadata filenames is returned only when one of them is evaluated. The choice
   * of which is irrelevant.
   * Since the metadata files for 1000Genomes are few and relate to all the samples, the interesting attributes are stored
   * as class variable for performance reasons.
   *
   * @param filename name of a file from the ones successfully produced during the download stage.
   * @param dataset  dataset of origin of the file
   * @param source   source of origin of this file
   * @return a List of Strings, i.e. the names of the files expected from the future transformation of the argument filename.
   */
  override def getCandidateNames(filename: String, dataset: Dataset, source: Source): List[String] = {
    if(this.dataset == null) {
      // read XML config parameters here because I need Dataset
      this.dataset = dataset
      datasetParameters = getDatasetParameters(dataset)
      mutationPrinter = SchemaAdapter.fromSchema(dataset.schemaUrl)
    } else if(this.dataset.name != dataset.name)
      throw new IllegalStateException("IT'S NOT THREAD SAFE TO REUSE THIS CLASS FOR MULTIPLE DATASETS. CREATE A NEW INSTANCE INSTEAD.")
    // fields used in match expressions needs to be stable identifiers, i.e. class val, or local val with first character uppercase
    val TreeFileName = datasetParameters("treeFileName")
    val SeqIndexMetaName = datasetParameters("seqIndexMetaName")
    val PopulationMetaName = datasetParameters("populationMetaName")
    val IndividualDetailsMetaName = datasetParameters("individualDetailsMetaName")
    val SamplesOriginMetaName = datasetParameters("samplesOriginMetaName")
    val downloadDirPath = dataset.fullDatasetOutputFolder+File.separator+"Downloads"+File.separator
    val trueFilename = removeCopyNumber(filename)
    trueFilename match {
      /* metadata files are few and shared between all samples. Instead of scanning 2,5K times the same files, the
       * relevant information is extracted and saved as class fields for later access */
      case TreeFileName =>
        logger.debug("EXTRACTION OF CANDIDATES FROM TREE FILE ("+trueFilename+")")
        List.empty[String]  // tree file doesn't contain any informative attribute
      case PopulationMetaName =>
        logger.debug("EXTRACTION OF CANDIDATES FROM POPULATION META ("+PopulationMetaName+")")
        populationMetadata = getPopulationMetadata(dataset, PopulationMetaName)
        List.empty[String]
      case SeqIndexMetaName =>
        logger.debug("EXTRACTION OF CANDIDATES FROM SEQ INDEX META ("+SeqIndexMetaName+")")
        sequencingMetadata = getSequencingMetadata(dataset, SeqIndexMetaName)
        List.empty[String]
      case IndividualDetailsMetaName =>
        logger.debug("EXTRACTION OF CANDIDATES FROM INDIVIDUAL DETAILS META ("+IndividualDetailsMetaName+")")
        individualsMetadata = getIndividualDetailsMetadata(dataset, IndividualDetailsMetaName)
        individualsMetadata.keys.toList.map(sample => s"$sample.gdm.meta")
      case SamplesOriginMetaName =>
        logger.debug("EXTRACTION OF CANDIDATES FROM SAMPLE ORIGIN META ("+SamplesOriginMetaName+")")
        samplesOriginMetadata = getSamplesOriginMetadata(dataset, SamplesOriginMetaName)
        List.empty[String]
      case variantCallArchive if variantCallArchive.endsWith("gz") =>
        logger.debug("EXTRACTION OF CANDIDATES FROM VARIANT ARCHIVE ("+variantCallArchive+")")
        // unzip
        extractArchive(downloadDirPath+variantCallArchive, downloadDirPath+removeExtension(variantCallArchive)) match {
          case None => List.empty[String]
          case Some(_VCFPath) =>
            // read sample names and output resulting filenames:
            new VCFAdapter(_VCFPath).biosamples.map(sample => s"$sample.gdm").toList
        }
      case unexpectedOriginFile =>
        throw new IllegalArgumentException("UNEXPECTED ORIGIN FILE "+unexpectedOriginFile+". ALL ORIGIN FILES MUST BE" +
          "HANDLED PROPERLY IN getCandidateNames.")
    }
  }

  ///////////////////////////////   METADATA EXTRACTION METHODS    ///////////////////////////////////////
  /**
   * Assign values to the named keys in the field datasetParameters.
   *
   * @param dataset the dataset where to get the XML parameters from
   * @throws MissingArgumentException if one of the following required XML parameters is missing:
   *                                  tree_file_url
   *                                  sequence_index_file_path
   *                                  population_file_path
   *                                  individual_details_file_path
   *                                  samples_origin_file_path
   *                                  assembly
   *                                  manually_curated&#95;&#95;pipeline
   */
  protected def getDatasetParameters(dataset: Dataset): mutable.LinkedHashMap[String, String]  = {
    val assignments = Vector(
      DatasetInfo.parseFilenameFromURL(dataset.getParameter("tree_file_url").getOrElse(
        throw new MissingArgumentException("MANDATORY PARAMETER tree_file_url NOT FOUND IN XML CONFIG FILE"))),
      DatasetInfo.parseFilenameFromURL(dataset.getParameter("sequence_index_file_path").getOrElse(
        throw new MissingArgumentException("MANDATORY PARAMETER sequence_index_file_path NOT FOUND IN XML CONFIG FILE"))),
      DatasetInfo.parseFilenameFromURL(dataset.getParameter("population_file_path").getOrElse(
        throw new MissingArgumentException("MANDATORY PARAMETER population_file_path NOT FOUND IN XML CONFIG FILE"))),
      DatasetInfo.parseFilenameFromURL(dataset.getParameter("individual_details_file_path").getOrElse(
        throw new MissingArgumentException("MANDATORY PARAMETER region_schema_file_path NOT FOUND IN XML CONFIG FILE"))),
      dataset.schemaUrl,
      dataset.getParameter("assembly").getOrElse(
        throw new MissingArgumentException("REQUIRED PARAMETER assembly IS MISSING FROM XML CONFIGURATION FILE")),
      dataset.getParameter("manually_curated__pipeline").getOrElse(
        throw new MissingArgumentException("REQUIRED PARAMETER manually_curated__pipeline IS MISSING FROM XML CONFIGURATION FILE")
      ),
      DatasetInfo.parseFilenameFromURL(dataset.getParameter("samples_origin_file_path").getOrElse(
        throw new MissingArgumentException("MANDATORY PARAMETER samples_origin_file_path NOT FOUND IN XML CONFIG FILE")))
    )
    //noinspection ScalaUnusedSymbol
    (datasetParameters zip assignments).map({ case ((key, oldValue), newValue) => key -> newValue })
  }

  /**
   * Reads the sequence.index metadata file, extracts the interesting attributes and builds a map (specifically a
   * ManyToFewMap instance) containing those values.
   * Currently selected attributes are: study id, study name, center mame, sample_id (!=sample name), population,
   * experiment id, instrument platform, instrument model, insert size, library layout, analysis group.
   * Attributes are indexed by sample name.
   * @param dataset an instance of Dataset.
   * @param seqIndexMetaName the name of the file with extension sequence.index inside the Download folder.
   * @return a Map of the interesting attributes by sample.
   */
  protected def getSequencingMetadata(dataset: Dataset, seqIndexMetaName: String): ManyToFewMap[String, List[String]] = {
    val reader = FileUtil.open(DatasetInfo.getDownloadDir(dataset)+seqIndexMetaName).get
    // skip headers
    var headerLine = reader.readLine()
    while (headerLine.startsWith("##"))
      headerLine = reader.readLine()
    // ANY CHANGE TO THE LIST OF INTERESTING ATTRIBUTES MUST BE REFLECTED ALSO IN METHOD TRANSFORM.
    val sequenceIndexMap = readTSVFileAsManyToFewMap(reader, interestingColumnsIdx = List(3, 4, 5, 8, 10, 11, 12, 13, 17, 18, 25), mapByColumnIndex = 9, 26)
    reader.close()
    /*
     map's values are uniformly sized Lists of Strings, where each list represents a different tuple of column values
     Those lists can contain duplicate elements. From N tuples I want to have 1 tuple with values merged by column
     with duplicates removed and different values separated with a comma.
     */
    mergeDuplicateValuesByColumn(sequenceIndexMap, ", ")
  }

  /**
   * Reads the populations.tsv file and extracts the interesting attributes.
   * Currently extracted attributes are: Population Code, Super Population, DNA from Blood.
   * Map's keys/values are added without leading/trailing whitespaces.
   * @param dataset an instance of Dataset.
   * @param populationMetaName the name of the populations.tsv file located in Downloads folder.
   * @return a map of the extracted attributes indexed by population code.
   */
  protected def getPopulationMetadata(dataset: Dataset, populationMetaName: String): Map[String, List[String]] = {
    val reader = FileUtil.open(DatasetInfo.getDownloadDir(dataset)+populationMetaName).get
    val pattern = PatternMatch.createPattern(".*\\t(\\S+)\\t(\\S+)\\t(yes|no)\\t(yes|no)\\t\\d+\\t\\d+\\t\\d+\\t\\d+")
    val matchingParts: List[List[String]] = PatternMatch.getLinesMatching(pattern, reader).map(line => PatternMatch.matchParts(line, pattern))
    reader.close()
    // ANY CHANGE TO THE LIST OF INTERESTING ATTRIBUTES MUST BE REFLECTED ALSO IN METHOD TRANSFORM.
    val keys = matchingParts.map(line => line.head.trim)
    val values = matchingParts.map(line => List(line(1).trim, line(2).trim))
    (keys zip values).toMap
  }

  /**
   * Reads a a .PED file and extracts the interesting attributes. The currently extracted attributes are:
   * Family ID and Gender.
   * @param dataset an instance of Dataset.
   * @param individualDetailsMetaName the name of the file with extension .PED located in Downloads folder.
   * @return a map (precisely ManyToFewMap) indexed by sample name and containing the extracted attributes.
   */
  protected def getIndividualDetailsMetadata(dataset: Dataset, individualDetailsMetaName: String): ManyToFewMap[String, List[String]] = {
    val reader = FileUtil.open(DatasetInfo.getDownloadDir(dataset)+individualDetailsMetaName).get
    // skip header line
    reader.readLine()
    // ANY CHANGE TO THE LIST OF INTERESTING ATTRIBUTES MUST BE REFLECTED ALSO IN METHOD TRANSFORM.
    val individualDetailsMap = readTSVFileAsManyToFewMap(reader, interestingColumnsIdx = List(0, 4), mapByColumnIndex = 1, 17)
      .transformValues((_, valueList) => {
        valueList.flatMap(row => List(
          row.head,
          if(row(1) == "1") "male" else "female"))
      })
    reader.close()
    individualDetailsMap
  }

  /**
   * Reads a sample_info.txt metadata file and extracts the attribute named DNA Source from Coriell (= "blood"|"lcl"|"")
    * @param dataset an instance of Dataset.
   * @param samplesOriginMetaName the name of the sample_info.txt file located in Downlaods folder.
   * @return a ManyToFewMap, telling for each sample the value of the attribute "DNA Source from Coriell".
   */
  protected def getSamplesOriginMetadata(dataset: Dataset, samplesOriginMetaName:String): ManyToFewMap[String, String] ={
    val reader = FileUtil.open(DatasetInfo.getDownloadDir(dataset)+samplesOriginMetaName).get
    // skip header line
    reader.readLine()
    // ANY CHANGE TO THE LIST OF INTERESTING ATTRIBUTES MUST BE REFLECTED ALSO IN METHOD TRANSFORM.
    val samplesOriginMap = readTSVFileAsManyToFewMap(reader, interestingColumnsIdx = List(59), mapByColumnIndex = 0, 61)
      .transformValues((_, valueList) => valueList.head.head.toLowerCase.trim) // valueList is expected to contain only one String
    reader.close()
    samplesOriginMap
  }

  /**
   * Reads a structured file with tab-separated values and generates a Map (ManyToFewMap) containing a List of values
   * of the columns indices interestingColumnsIdx (counting from 0) indexed by the column index
   * mapByColumnIndex. Empty rows are skipped and Map's keys/values are added without leading/trailing whitespaces.
   * @param reader a buffered reader for the file to read.
   * @param interestingColumnsIdx a List of the column indices of the attributes to be stored in the map for each key.
   * @param mapByColumnIndex the index of the column to be used as key for the generated map.
   * @param maxNumberOfColumns the expected number of columns in the file. It's used to limit the number of attributes
   *                           selected in each row and to check that each line is correctly formatted.
   * @return a Map having a List of selected attributes as value and the attribute in mapByColumnIndex as key.
   */
  protected def readTSVFileAsManyToFewMap(reader: BufferedReader, interestingColumnsIdx: List[Int], mapByColumnIndex: Int, maxNumberOfColumns: Int): ManyToFewMap[String, List[String]] = {
    val mtfMap = new ManyToFewMap[String, List[String]]
    // fill the map
    FileUtil.scanFileAndClose(reader, line => {
      try {
        if(!line.trim.isEmpty) {
          val lineFields = line.split("\t", -1)
          if(lineFields.size != maxNumberOfColumns)
            throw new IndexOutOfBoundsException("EXPECTED "+maxNumberOfColumns+" COLUMNS, INSTEAD THERE WERE "+lineFields.size)
          val values: List[String] = interestingColumnsIdx.map(i => lineFields(i).trim)
          mtfMap.add(lineFields(mapByColumnIndex).trim, values)
        }
      } catch {
        case e: Exception =>
          logger.error("TSV PARSER ERROR. SKIPPED LINE: "+line, e)
      }
    })
    mtfMap
  }

  /**
   * A ManyToFew of type [K, V] returns a list of Vs, for each key. This method transforms a ManyToFewMap by merging
   * multiple values into a single value of the same type by keeping only one of them if they're equal, or by joining
   * the values with valueSeparator if they're different. This method does so specifically for ManyToFewMap with
   * value of type List[String] by considering the strings inside the list as column attributes and thus
   * merging the values by column.
   * @param mtfMap an instance of ManyToFewMap of type String, List[String].
   * @param valueSeparator a separator string.
   * @return the same input object with values merged by column, considering the indices of List[String] as columns.
   */
  protected def mergeDuplicateValuesByColumn(mtfMap: ManyToFewMap[String, List[String]], valueSeparator: String): ManyToFewMap[String, List[String]] ={
    mtfMap.transformValues((_, values) => {
      /* treat the values as rows, each row with a list of attributes. Returns a single row with the all the columns
      * but only one attribute per column */
      val columnIndices = values.head.indices.toList
      columnIndices.map(colIndex => {
        val columnValues = values.map(row => row(colIndex))
        columnValues.distinct.mkString(valueSeparator)  // concatenate different column values with a separator
      })
    })
  }

  /**
   * Initialize the field manually_curated&#95;&#95;data_url with the URLs of all the source VCF files from which
   * region data comes.
   * @return the URLs of all the source VCF files from which region data comes.
   */
  //noinspection AccessorLikeMethodIsEmptyParen
  protected def getURLsOfOriginRegionData(): List[String] = {
    if(manually_curated__data_url == null)
      manually_curated__data_url = {
      val originRegionFileNames = extractedArchives.keys.map(FileUtil.getFileNameFromPath) //names without copy number
      val datasetID = FileDatabase.datasetId(FileDatabase.sourceId(dataset.source.name), dataset.name)
      originRegionFileNames.toList.sorted.map(nameOfCompressedOrigin => FileDatabase.getFileUrl(nameOfCompressedOrigin, datasetID, it.polimi.genomics.metadata.database.Stage.DOWNLOAD))
    }
    manually_curated__data_url
  }

/*
  private def TESTwriteMetadata(outputDirectoryPath: String):Unit ={
    // population
    if(populationMetadata.nonEmpty) {
      val writer = FileUtil.writeReplace(outputDirectoryPath + "population").get
      writer.write("POP\tSUPER-POP\tDNA from blood")
      writer.newLine()
      populationMetadata.foreach(pop => {
        writer.write(pop._1 + "\t" + pop._2.mkString("\t"))
        writer.newLine()
      })
      writer.close()
    }
    // sequencing
    if(sequencingMetadata != null) {
      val writer = FileUtil.writeReplace(outputDirectoryPath + "sequencing").get
      writer.write("SAMPLE\tSTUDY_NAME\tPOPULATION\tINSTRUMENT_PLATFORM\tINSTRUMENT_MODEL\tANALYSIS_GROUP")
      writer.newLine()
      sequencingMetadata.foreach((sample, attrs) => {
        writer.write(sample+"\t"+attrs.mkString("\t"))
        writer.newLine()
      })
      writer.close()
    }
    // individual details
    if(individualsMetadata != null) {
      val writer = FileUtil.writeReplace(outputDirectoryPath + "individual_details").get
      writer.write("SAMPLE_ID\tGENDER")
      writer.newLine()
      individualsMetadata.foreach((sample, gender) => {
        writer.write(sample + "\t" + gender.mkString("\t"))
        writer.newLine()
      })
      writer.close()
    }
  }
*/

  ///////////////////////////////   GENERIC HELPER METHODS    ///////////////////////////////////////
  /**
   * Extracts the archive at the given destination file path as a single file, overwriting an already existing file with
   * the same target file path. However if the same archive has already been extracted during this session,
   * the extraction is skipped and the method returns true.
   * The extraction fails if a directory already exists at the target file path.
   */
  protected def extractArchive(archivePath: String, extractedFilePath: String):Option[String] ={
    extractedArchives.get(archivePath).orElse({
      logger.info("EXTRACTING "+archivePath.substring(archivePath.lastIndexOf(File.separator)))
      if (Unzipper.unGzipIt(archivePath, extractedFilePath)) {
        extractedArchives += (archivePath -> extractedFilePath)
        Some(extractedFilePath)
      } else {
        logger.error("EXTRACTION FAILED. THE PACKAGE IS PROBABLY DAMAGED OR INCOMPLETE. " +
          "SKIP TRANSFORMATION FOR THIS PACKAGE.")
        None
      }
    })
  }

}
object OneKGTransformer {

  val META_KEY_VALUE_SEPARATOR = "\t"
  val MISSING_METADATA_VALUE = "*"

  ///////////////////////////////   GENERIC HELPER METHODS    ///////////////////////////////////////

  def removeCopyNumber(filename: String): String = {
    filename.replaceFirst("_\\d\\.", ".")
  }

  def removeExtension(filename: String): String ={
    filename.substring(0, filename.lastIndexOf("."))
  }

  //////////////////////////////    STRING FORMATTING FUNCTIONS   /////////////////////////////////////

  /**
   * Generates a string of tab-separated values.
   */
  def makeTSVString(words: String*): String ={
    words.mkString("\t")
  }

  /**
   * Generates a string of tab-separated values.
   */
  def makeTSVString(words: List[String]):String ={
    words.mkString("\t")
  }

  /**
   * Generates a string of tab-separated values, adding a tab also at the beginning of the string.
   */
  def concatTSVString(words: String*):String ={
    "\t".concat(words.mkString("\t"))
  }

  /**
   * Generates a string of tab-separated values, adding a tab also at the beginning of the string.
   */
  def concatTSVString(words: List[String]):String ={
    "\t".concat(words.mkString("\t"))
  }

  def addToMetadataContainer(container: util.TreeMap[String, mutable.Set[String]], key:String, value: String):util.TreeMap[String, mutable.Set[String]] ={
    val newValues: mutable.Set[String] = Option(container.get(key)).getOrElse(mutable.Set.empty[String]) += value
    container.put(key, newValues)
    container
  }

  def addToMetadataContainer(container: util.TreeMap[String, mutable.Set[String]], keys:List[String], values: List[String]):util.TreeMap[String, mutable.Set[String]] ={
    (keys zip values).foreach({ case (key, value) =>
      val newValues: mutable.Set[String] = Option(container.get(key)).getOrElse(mutable.Set.empty[String]) += value
      container.put(key, newValues)
    })
    container
  }

  def writeMetadataContainer(container: util.TreeMap[String, mutable.Set[String]], intoFilePath: String):Unit ={
    val writer = FileUtil.writeReplace(intoFilePath).get // throws exception if fails
    import scala.collection.JavaConversions._
    for (entry <- container.entrySet) {
      entry.getValue.foreach(value => {
        writer.write(entry.getKey + META_KEY_VALUE_SEPARATOR + value)
        writer.newLine()
      })
    }
    writer.close()
  }

  def noValueList(dimension: Int): List[String] ={
    List.fill(dimension)(MISSING_METADATA_VALUE)
  }

  def indicesOfTSVString(TSVString:String, columnsOfInterest: String):List[Int] = {
    val interestingAttrs_1 = columnsOfInterest.toLowerCase
    TSVString.split("\t", -1).zipWithIndex.collect{
      case (name, idx) if interestingAttrs_1.contains(name.trim.toLowerCase) => idx
    }.toList
  }
}
