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
 * Transformer for source 1000Genomes
 */
class OneKGTransformer extends Transformer {
  import OneKGTransformer._

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  // metadata file names
  private var XMLParams = None : Option[(String, String, String, String)]
  private lazy val treeFileName:String = XMLParams.get._1
  private lazy val seqIndexMetaName:String = XMLParams.get._2
  private lazy val populationMetaName:String = XMLParams.get._3
  private lazy val individualDetailsMetaName:String = XMLParams.get._4
  // remember extracted archives
  private var extractedArchives: ListBuffer[String] = ListBuffer.empty[String]
  private var VCFs: Map[String, VCFAdapter] = Map.empty[String, VCFAdapter]
  // metadata
  private var sequencingMetadata: ManyToFewMap[String, List[String]] = _
  private var populationMetadata: Map[String, List[String]] = _
  private var individualsMetadata: ManyToFewMap[String, List[String]] = _
  private var assembly:Option[String] = None


  /**
   * From a downloaded file, writes the given candidate file as a metadata or region file.
   *
   * @param source           source where the files belong to.
   * @param originPath       path for the  "Downloads" folder without trailing file separator
   * @param destinationPath  path for the "Transformations" folder without trailing file separator
   * @param originalFilename name of a compressed VCF file or metadata file
   * @param filename         name of the region data or metadata file to create and populate with the attributes of the origin.
   * @return true if the transformation produced a file, false otherwise (the candidate filename will be marked as failed)
   */
  override def transform(source: Source, originPath: String, destinationPath: String, originalFilename: String, filename: String): Boolean = {
    val sourceFileName = removeCopyNumber(originalFilename)
    val targetFileName = filename
    val sourceFilePath = originPath+File.separator+sourceFileName
    val targetFilePath = destinationPath+File.separator+targetFileName

    if (filename.endsWith(".gdm")) {
      // append region data
      val unzippedVCFFilePath = removeExtension(sourceFilePath)
      if(!extractArchive(sourceFilePath, unzippedVCFFilePath))
        return false
      else {
        val sampleName = removeExtension(targetFileName)
        val sourceVCF = VCFs(sourceFilePath)
        sourceVCF.appendMutationsOf(sampleName, targetFilePath)
      }
    } else {
      val sampleName = removeExtension(removeExtension(targetFileName))
      // write all metadata
      val writer = FileUtil.writeReplace(targetFilePath).get // throws exception if fails
      // assembly
      writeMetadata(writer, onNewLine = false, "assembly", Try(assembly.get).getOrElse(MISSING_VALUE))
      // study name, population, instrument platform and model, analysis group
      val thisSampleSequencingData = Try(sequencingMetadata.getFirstOf(sampleName).get)
      val population = Try(thisSampleSequencingData.get(1).toString)
      writeMetadata(writer, onNewLine = true,
        List("study_name", "population", "instrument_platform", "instrument_model", "analysis_group"), thisSampleSequencingData.getOrElse({
                logger.error("SEQUENCING DATA FOR SAMPLE "+sampleName+" NOT FOUND")
                noValueList(5)}))
      // super-population, dna from blood
      writeMetadata(writer, onNewLine = true,
        List("super_population", "DNA_from_blood"), Try(populationMetadata(population.get)).getOrElse({
                logger.error("POPULATION DATA FOR POPULATION "+sampleName+" NOT FOUND")
                noValueList(2)
              }))
      // gender
      writeMetadata(writer, onNewLine = true, "gender", Try(individualsMetadata.getFirstOf(sampleName).get.head).getOrElse(MISSING_VALUE))
      writer.newLine()  // prevents TransformerStep from concatenating the attribute "manually_curated__local_file_size" on last line
      writer.close()
    }
//  val loggerWriter = FileUtil.writeAppend("Example/examples_meta/1kGenomes/transform_log", true).get
//  loggerWriter.write(targetFileName)
//  loggerWriter.close()
  true
  }

  /**
   * by receiving an original filename returns the new GDM candidate name(s).
   * The region file must be before than related meta file
   *
   * @param filename original filename
   * @param dataset  dataset where the file belongs to
   * @param source   source where the files belong to.
   * @return candidate names for the files derived from the original filename.
   */
  override def getCandidateNames(filename: String, dataset: Dataset, source: Source): List[String] = {
    // read XML config parameters here because I need Dataset
    initMetadataFileNames(dataset)
    if(assembly.isEmpty)
      assembly = Some(getAssembly(dataset))
    val downloadDirPath = s"${dataset.source.outputFolder}\\${dataset.outputFolder}\\Downloads\\"
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
        val archivePath = downloadDirPath+trueFilename
        val VCFFilePath = downloadDirPath+removeExtension(trueFilename)
        if(!extractArchive(archivePath, VCFFilePath))
          List.empty[String]
        // read sample names and output resulting filenames:
        val vcf = new VCFAdapter(VCFFilePath)
        VCFs += (archivePath -> vcf)
        vcf.biosamples.map(sample => s"$sample.gdm")
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
  def initMetadataFileNames(dataset: Dataset): Unit = {
    if (XMLParams.isEmpty) XMLParams = Some(
      FileUtil.getFileNameFromPath(dataset.getParameter("tree_file_url").getOrElse(
        throw new MissingArgumentException("MANDATORY PARAMETER tree_file_url NOT FOUND IN XML CONFIG FILE"))),
      FileUtil.getFileNameFromPath(dataset.getParameter("sequence_index_file_path").getOrElse(
        throw new MissingArgumentException("MANDATORY PARAMETER sequence_index_file_path NOT FOUND IN XML CONFIG FILE"))),
      FileUtil.getFileNameFromPath(dataset.getParameter("population_file_path").getOrElse(
        throw new MissingArgumentException("MANDATORY PARAMETER population_file_path NOT FOUND IN XML CONFIG FILE"))),
      FileUtil.getFileNameFromPath(dataset.getParameter("individual_details_file_path").getOrElse(
        throw new MissingArgumentException("MANDATORY PARAMETER individual_details_file_path NOT FOUND IN XML CONFIG FILE")))
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
    val sequenceIndexMap = readTSVFileAsManyToFewMap(reader, interestingColumnsIdx = List(4, 10, 12, 13, 25), mapByColumnIndex = 9)
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
    readTSVFileAsManyToFewMap(reader, interestingColumnsIdx = List(4), mapByColumnIndex = 1)
      .transformValues((_, valueList) => {
        valueList.flatMap(row => List(if(row.head == "1") "male" else "female"))
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

  /*  METHOD USED ONLY FOR DEBUGGING  */
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

  ///////////////////////////////   GENERIC HELPER METHODS    ///////////////////////////////////////
  /**
   * Extracts the archive at the given destination file path as a single file, overwriting an already existing file with
   * the same target file path. However if the same archive has already been extracted during this session,
   * the extraction is skipped and the method returns true.
   * The extraction fails if a directory already exists at the target file path.
   */
  def extractArchive(archivePath: String, extractedFilePath: String):Boolean ={
    logger.info("EXTRACTING "+archivePath.substring(archivePath.lastIndexOf(File.separator)))
    if(extractedArchives.contains(extractedFilePath))
      true
    else if(Unzipper.unGzipIt(archivePath, extractedFilePath)){
      extractedArchives += extractedFilePath
      true
    } else {
      logger.error("EXTRACTION FAILED. THE PACKAGE IS PROBABLY DAMAGED OR INCOMPLETE. " +
        "SKIP TRANSFORMATION FOR THIS PACKAGE.")
      false
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

  //TODO change names key, value. Change method name. Change code in String.join
  def tabber(words: String*): String ={
    words.reduce((key, value) => key+"\t"+value)
  }

  def tabber(words: List[String]):String ={
    words.reduce((key, value) => key+"\t"+value)
  }

  def tabberConcat(words: String*):String ={
    "\t".concat(words.reduce((key, value) => key+"\t"+value))
  }

  def tabberConcat(words: List[String]):String ={
    "\t".concat(words.reduce((key, value) => key+"\t"+value))
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
