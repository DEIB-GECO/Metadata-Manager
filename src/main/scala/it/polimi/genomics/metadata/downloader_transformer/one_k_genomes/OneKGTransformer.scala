package it.polimi.genomics.metadata.downloader_transformer.one_k_genomes

import java.io.{BufferedReader, BufferedWriter, File}

import it.polimi.genomics.metadata.downloader_transformer.Transformer
import it.polimi.genomics.metadata.downloader_transformer.default.utils.Unzipper
import it.polimi.genomics.metadata.step.xml.{Dataset, Source}
import it.polimi.genomics.metadata.util.{FileUtil, ManyToFewMap, PatternMatch}
import org.apache.commons.cli.MissingArgumentException
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer
import scala.util.Try

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
  // metadata file names
  private var XMLParams = None : Option[(String, String, String, String, String)]
  private lazy val treeFileName:String = XMLParams.get._1
  private lazy val seqIndexMetaName:String = XMLParams.get._2
  private lazy val populationMetaName:String = XMLParams.get._3
  private lazy val individualDetailsMetaName:String = XMLParams.get._4
  // remember extracted archives & early transformed VCFs
  private var extractedArchives: Map[String, String] = Map.empty[String, String]
  private var transformedVCFs: ListBuffer[String] = ListBuffer.empty[String]
  // metadata
  private var sequencingMetadata: ManyToFewMap[String, List[String]] = _
  private var populationMetadata: Map[String, List[String]] = _
  private var individualsMetadata: ManyToFewMap[String, List[String]] = _
  private var assembly:Option[String] = None
  // formatting options
  private lazy val mutationPrinter: MutationPrinterTrait = SchemaAdapter.fromSchema(XMLParams.get._5)


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

    if (filename.endsWith(".gdm")) {
      // append region data
      extractArchive(sourceFilePath, removeExtension(sourceFilePath)) match {
        case None => return false
        case Some(_VCFPath) =>
          // the source file is completely transformed generating the target region files for all the samples available in the source file
          if(!transformedVCFs.contains(_VCFPath)){
            new VCFAdapter(_VCFPath, mutationPrinter)
              .appendAllMutationsBySample(transformationsDirPath)
            transformedVCFs += _VCFPath
          }
      }
    } else {
      val sampleName = removeExtension(removeExtension(targetFileName))
      // write all metadata
      val writer = FileUtil.writeReplace(targetFilePath).get // throws exception if fails
      // assembly
      writeMetadata(writer, onNewLine = false, "assembly", Try(assembly.get).getOrElse(MISSING_VALUE))
      // study, center mame, sample_id (!=sample name), population, experiment, instrument, insert size, library, analysis group
      val thisSampleSequencingData = Try(sequencingMetadata.getFirstOf(sampleName).get)
      val population = Try(thisSampleSequencingData.get(1).toString)
      writeMetadata(writer, onNewLine = true,
        List("study_id", "study_name", "center_name", "sample_id", "population", "experiment_id", "instrument_platform",
          "instrument_model", "insert_size", "library_layout", "analysis_group"),
        thisSampleSequencingData.getOrElse({
                logger.error("SEQUENCING DATA FOR SAMPLE "+sampleName+" NOT FOUND")
                noValueList(5)}))
      // super-population, dna from blood
      writeMetadata(writer, onNewLine = true,
        List("super_population", "DNA_from_blood"), Try(populationMetadata(population.get)).getOrElse({
                logger.error("POPULATION DATA FOR POPULATION "+sampleName+" NOT FOUND")
                noValueList(2)
              }))
      // family id, gender
      writeMetadata(writer, onNewLine = true,
        List("family_id", "gender"),
        Try(individualsMetadata.getFirstOf(sampleName).get).getOrElse(noValueList(2)))
      writer.newLine()  // prevents TransformerStep from concatenating the attribute "manually_curated__local_file_size" on last line
      writer.close()
    }
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
    // read XML config parameters here because I need Dataset
    initMetadataFileNamesAndFormattingOptions(dataset)
    if(assembly.isEmpty)
      assembly = Some(getAssembly(dataset))
    val downloadDirPath = dataset.fullDatasetOutputFolder+File.separator+"Downloads"+File.separator
    val trueFilename = removeCopyNumber(filename)
    //noinspection ScalaUnusedSymbol
    trueFilename match {
      /* metadata files are few and shared between all samples. Instead of scanning 2,5K times the same files, the
       * relevant information is extracted and saved as class fields for later access */
      case this.treeFileName =>
        List.empty[String]  // tree file doesn't contain any informative attribute
      case this.populationMetaName =>
        populationMetadata = getPopulationMetadata(dataset)
        List.empty[String]
      case this.seqIndexMetaName =>
        sequencingMetadata = getSequencingMetadata(dataset)
        List.empty[String]
      case this.individualDetailsMetaName =>
        individualsMetadata = getIndividualDetailsMetadata(dataset)
        individualsMetadata.keys.toList.map(sample => s"$sample.gdm.meta")
      case variantCallArchive =>
        // unzip
        extractArchive(downloadDirPath+trueFilename, downloadDirPath+removeExtension(trueFilename)) match {
          case None => List.empty[String]
          case Some(_VCFPath) =>
            // read sample names and output resulting filenames:
            new VCFAdapter(_VCFPath).biosamples.map(sample => s"$sample.gdm").toList
        }
    }
  }

  ///////////////////////////////   METADATA EXTRACTION METHODS    ///////////////////////////////////////
  /**
   * Initialize the filed XMLParams and consequently also the individual fields of metadata filenames.
   *
   * @param dataset the dataset where to get the XML parameters from
   * @throws MissingArgumentException if one of the following required XML parameters is missing:
   *                                  tree_file_url
   *                                  sequence_index_file_path
   *                                  population_file_path
   *                                  individual_details_file_path
   */
  def initMetadataFileNamesAndFormattingOptions(dataset: Dataset): Unit = {
    if (XMLParams.isEmpty) XMLParams = Some(
      DatasetInfo.parseFilenameFromURL(dataset.getParameter("tree_file_url").getOrElse(
        throw new MissingArgumentException("MANDATORY PARAMETER tree_file_url NOT FOUND IN XML CONFIG FILE"))),
      DatasetInfo.parseFilenameFromURL(dataset.getParameter("sequence_index_file_path").getOrElse(
        throw new MissingArgumentException("MANDATORY PARAMETER sequence_index_file_path NOT FOUND IN XML CONFIG FILE"))),
      DatasetInfo.parseFilenameFromURL(dataset.getParameter("population_file_path").getOrElse(
        throw new MissingArgumentException("MANDATORY PARAMETER population_file_path NOT FOUND IN XML CONFIG FILE"))),
      DatasetInfo.parseFilenameFromURL(dataset.getParameter("individual_details_file_path").getOrElse(
        throw new MissingArgumentException("MANDATORY PARAMETER region_schema_file_path NOT FOUND IN XML CONFIG FILE"))),
      dataset.schemaUrl
    )
  }

  def getAssembly(dataset: Dataset):String ={
    dataset.getParameter("assembly").getOrElse(throw new MissingArgumentException("REQUIRED" +
      " PARAMETER assembly IS MISSING FROM XML CONFIGURATION FILE"))
  }

  def getSequencingMetadata(dataset: Dataset): ManyToFewMap[String, List[String]] = {
    val reader = FileUtil.open(DatasetInfo.getDownloadDir(dataset)+seqIndexMetaName).get
    if(dataset.name.equals("GRCh38")){
      var headerLine = reader.readLine()
      while (headerLine.startsWith("##"))
        headerLine = reader.readLine()
    }
    // skip header line
    reader.readLine()
    val sequenceIndexMap = readTSVFileAsManyToFewMap(reader, interestingColumnsIdx = List(3, 4, 5, 8, 10, 11, 12, 13, 17, 18, 25), mapByColumnIndex = 9)
    /*
     map's values are uniformly sized Lists of Strings, where each list represents a different tuple of column values
     Those lists can contain duplicate elements. From N tuples I want to have 1 tuple with values merged by column
     with duplicates removed and different values separated with a comma.
     */
    mergeDuplicateValuesByColumn(sequenceIndexMap, ", ")
  }

  def getPopulationMetadata(dataset: Dataset): Map[String, List[String]] = {
    val reader = FileUtil.open(DatasetInfo.getDownloadDir(dataset)+populationMetaName).get
    val pattern = PatternMatch.createPattern(".*\\t(\\S+)\\t(\\S+)\\t(yes|no)\\t(yes|no)\\t\\d+\\t\\d+\\t\\d+\\t\\d+")
    val matchingParts: List[List[String]] = PatternMatch.getLinesMatching(pattern, reader).map(line => PatternMatch.matchParts(line, pattern))
    val keys = matchingParts.map(line => line.head)
    val values = matchingParts.map(line => List(line(1), line(2)))
    (keys zip values).toMap
  }

  def getIndividualDetailsMetadata(dataset: Dataset): ManyToFewMap[String, List[String]] = {
    val reader = FileUtil.open(DatasetInfo.getDownloadDir(dataset)+individualDetailsMetaName).get
    // skip header line
    reader.readLine()
    readTSVFileAsManyToFewMap(reader, interestingColumnsIdx = List(0, 4), mapByColumnIndex = 1)
      .transformValues((_, valueList) => {
        valueList.flatMap(row => List(
          row.head,
          if(row(1) == "1") "male" else "female"))
      })
  }

  def readTSVFileAsManyToFewMap(reader: BufferedReader, interestingColumnsIdx: List[Int], mapByColumnIndex: Int): ManyToFewMap[String, List[String]] = {
    val mtfMap = new ManyToFewMap[String, List[String]]
    // fill the map
    FileUtil.scanFileAndClose(reader, line => {
      try {
        if(!line.trim.isEmpty) {
          val lineFields = line.split("\t")
          val values: List[String] = interestingColumnsIdx.map(i => lineFields(i))
          mtfMap.add(lineFields(mapByColumnIndex), values)
        }
      } catch {
        case e: Exception =>
          logger.error("TSV PARSER ERROR. SKIPPED LINE: "+line, e)
      }
    })
    mtfMap
  }

  def mergeDuplicateValuesByColumn(mtfMap: ManyToFewMap[String, List[String]], valueSeparator: String): ManyToFewMap[String, List[String]] ={
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
  def extractArchive(archivePath: String, extractedFilePath: String):Option[String] ={
  //TODO REMOVE THIS

//    if(archivePath == "C:\\Users\\tomma\\IntelliJ-Projects\\Metadata-Manager\\Example\\examples_meta\\1kGenomes\\GRCh38\\Downloads\\ALL.chrX.shapeit2_integrated_snvindels_v2a_27022019.GRCh38.phased.vcf.gz")
//      return Some("C:\\Users\\tomma\\IntelliJ-Projects\\Metadata-Manager\\Example\\examples_meta\\1kGenomes\\GRCh38\\Downloads\\ALL.chrX.shapeit2_integrated_snvindels_v2a_27022019.GRCh38.phased_short.vcf")
//    else if(archivePath == "C:\\Users\\tomma\\IntelliJ-Projects\\Metadata-Manager\\Example\\examples_meta\\1kGenomes\\GRCh38\\Downloads\\ALL.chr22.shapeit2_integrated_snvindels_v2a_27022019.GRCh38.phased.vcf.gz")
//      return Some("C:\\Users\\tomma\\IntelliJ-Projects\\Metadata-Manager\\Example\\examples_meta\\1kGenomes\\GRCh38\\Downloads\\ALL.chr22.shapeit2_integrated_snvindels_v2a_27022019.GRCh38.phased_short.vcf")
//    else

    logger.info("EXTRACTING "+archivePath.substring(archivePath.lastIndexOf(File.separator)))
    if(extractedArchives.contains(archivePath))
      Some(extractedArchives(archivePath))
    else if(Unzipper.unGzipIt(archivePath, extractedFilePath)){
      extractedArchives += (archivePath -> extractedFilePath)
      Some(extractedFilePath)
    } else {
      logger.error("EXTRACTION FAILED. THE PACKAGE IS PROBABLY DAMAGED OR INCOMPLETE. " +
        "SKIP TRANSFORMATION FOR THIS PACKAGE.")
      None
    }
  }

}
object OneKGTransformer {

  val META_KEY_VALUE_SEPARATOR = "\t"
  val MISSING_VALUE = "*"

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

  def writeMetadata(writer: BufferedWriter, onNewLine: Boolean, key: String, value: String): Unit ={
    if(onNewLine)
      writer.newLine()
    writer.write(key+META_KEY_VALUE_SEPARATOR+value)
  }

  def writeMetadata(writer: BufferedWriter, onNewLine: Boolean, keys: List[String], values: List[String]): Unit ={
    for(i <- keys.indices) {
      if(i==0 && onNewLine || i!=0)
        writer.newLine()
      writer.write(keys(i) + META_KEY_VALUE_SEPARATOR + values(i))
    }
  }

  def noValueList(dimension: Int): List[String] ={
    List.fill(dimension)(MISSING_VALUE)
  }

  //////////////////////////////    SOURCE SPECIFIC METHODS  ////////////////////////////////////////

  def parseAlgorithmName(VCFFileNameOrPath: String): String = {
    val algorithmName = new ListBuffer[String]()
    if(VCFFileNameOrPath.matches(".*callmom.*"))
      algorithmName += "callMom"
    if(VCFFileNameOrPath.matches(".*shapeit2.*"))
      algorithmName += "ShapeIt2"
    if(VCFFileNameOrPath.matches(".*mvncall.*"))
      algorithmName += "Mvncall"
    algorithmName.reduce((a1, a2) => a1+" + "+a2)
  }

}
