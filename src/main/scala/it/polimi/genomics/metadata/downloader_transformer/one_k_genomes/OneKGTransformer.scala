package it.polimi.genomics.metadata.downloader_transformer.one_k_genomes

import java.io.File

import it.polimi.genomics.metadata.downloader_transformer.Transformer
import it.polimi.genomics.metadata.downloader_transformer.default.utils.Unzipper
import it.polimi.genomics.metadata.step.xml.{Dataset, Source}
import it.polimi.genomics.metadata.util.FileUtil
import org.apache.commons.cli.MissingArgumentException
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

/**
 * Created by Tom on ott, 2019
 *
 * Transformer for source 1000Genomes
 */
class OneKGTransformer extends Transformer {
  import OneKGTransformer._

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private var XMLParams = None : Option[(String, String, String)]
  lazy val treeFileName:String = XMLParams.get._1
  lazy val seqIndexMetaName:String = XMLParams.get._2
  lazy val populationMetaName:String = XMLParams.get._3

  val extractedArchives = new ListBuffer[String]

  val META_ALGORITHM_KEY = "algorithm"

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
//    println("TRANSFORM CALLED FOR ORIGINAL FILE "+originalFilename+" WITH EXPECTED OUTPUT "+filename)
    val originFilePath = originPath+File.separator+originalFilename
    val targetFilePath = destinationPath+File.separator+filename
    // recognize origin file kind
    originalFilename match {
      case this.seqIndexMetaName =>
        // TODO add metadata to metadata of filename
      case compressedVCF =>
        val VCFFilePath = removeExtension(originFilePath)
        if(!extractArchive(originFilePath, VCFFilePath))
          return false
        if (isMetadata(filename)) {
          val writer = FileUtil.writeAppend(targetFilePath, startOnNewLine = true).get // throws exception if fails
          writer.write(tabber(META_ALGORITHM_KEY, parseAlgorithmName(originalFilename)))
          writer.close()
        } else {
          val sampleName = removeExtension(filename)
          val sourceVCF = new VCFAdapter(VCFFilePath)
          sourceVCF.appendMutationsOf(sampleName, targetFilePath)
        }
    }
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
    // decide what to return based on the kind of file
    val trueFilename = removeCopyNumber(filename)
    val downloadDirPath = dataset.fullDatasetOutputFolder+File.separator+"Downloads"+File.separator
    trueFilename match {
      case this.treeFileName => List.empty[String]
      case this.populationMetaName =>
      //TODO build a dictionary or map with required info
        List.empty[String]
      case this.seqIndexMetaName =>
     // TODO scan sequence index file and return sample.gdm.meta
        List("a.gdm", "a.gdm.meta")
      case variantCallArchive =>
        // unzip
        val archivePath = downloadDirPath+trueFilename
        val VCFFilePath = downloadDirPath+removeExtension(trueFilename)
        if(!extractArchive(archivePath, VCFFilePath))
          List.empty[String]
        // read sample names and output resulting filenames:
        val sampleFiles = (new VCFAdapter(VCFFilePath)).biosamples.map(sample => s"$sample.gdm")
        val metadataFiles = sampleFiles.map(sample => s"$sample.gdm.meta")
        sampleFiles:::metadataFiles
    }
  }

  ///////////////////////////////   GENERIC HELPER METHODS    ///////////////////////////////////////

  def initMetadataFileNames(dataset: Dataset): Unit = {
    if (XMLParams.isEmpty) XMLParams = Some(
      FileUtil.getFileNameFromPath(dataset.getParameter("tree_file_url").getOrElse(
        throw new MissingArgumentException("MANDATORY PARAMETER tree_file_url NOT FOUND IN XML CONFIG FILE"))),
      FileUtil.getFileNameFromPath(dataset.getParameter("sequence_index_file_path").getOrElse(
        throw new MissingArgumentException("MANDATORY PARAMETER sequence_index_file_path NOT FOUND IN XML CONFIG FILE"))),
    FileUtil.getFileNameFromPath(dataset.getParameter("population_file_path").getOrElse(
      throw new MissingArgumentException("MANDATORY PARAMETER population_file_path NOT FOUND IN XML CONFIG FILE")))
    )
  }

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

  ///////////////////////////////   GENERIC HELPER METHODS    ///////////////////////////////////////

  def removeCopyNumber(filename: String): String = {
    filename.replaceFirst("_\\d\\.", ".")
  }

  def removeExtension(filename: String): String ={
    filename.substring(0, filename.lastIndexOf("."))
  }

  def isMetadata(filename: String): Boolean ={
    filename.endsWith(".meta")
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

}
