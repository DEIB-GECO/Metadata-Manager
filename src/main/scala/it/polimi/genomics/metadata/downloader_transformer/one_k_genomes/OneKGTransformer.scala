package it.polimi.genomics.metadata.downloader_transformer.one_k_genomes

import java.io.{BufferedReader, File, Reader}
import java.nio.file.{Files, OpenOption, Paths, StandardCopyOption, StandardOpenOption}
import java.util.regex.Pattern

import it.polimi.genomics.metadata.downloader_transformer.Transformer
import it.polimi.genomics.metadata.downloader_transformer.default.utils.Unzipper
import it.polimi.genomics.metadata.downloader_transformer.one_k_genomes.VCFInfo.VariantPattern
import it.polimi.genomics.metadata.step.xml.{Dataset, Source}
import it.polimi.genomics.metadata.util.{FTPHelper, FileUtil, PatternMatch}
import org.apache.commons.cli.MissingArgumentException
import org.slf4j.{Logger, LoggerFactory}

/**
 * Created by Tom on ott, 2019
 */
class OneKGTransformer extends Transformer {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private var XMLParams = None : Option[(String, String, String)]
  lazy val treeFileName:String = XMLParams.get._1
  lazy val seqIndexMetaName:String = XMLParams.get._2
  lazy val populationMetaName:String = XMLParams.get._3


  /**
   * recieves .json and .bed.gz files and transform them to get metadata in .meta files and region in .bed files.
   *
   * @param source           source where the files belong to.
   * @param originPath       path for the  "Downloads" folder without trailing file separator
   * @param destinationPath  path for the "Transformations" folder without trailing file separator
   * @param originalFilename name of the file to be transformed
   * @param filename         name of the file to create during this transformation
   * @return true if the transformation produced a file, false otherwise (the candidate filename will be marked as failed)
   */
  override def transform(source: Source, originPath: String, destinationPath: String, originalFilename: String, filename: String): Boolean = {
//    println("TRANSFORM CALLED FOR ORIGINAL FILE "+originalFilename+" WITH EXPECTED OUTPUT "+filename)
    val originFilePath = originPath+File.separator+originalFilename
    val writer = FileUtil.writeAppend(destinationPath+File.separator+filename, startOnNewLine = true)
    // recognize original file kind
    originalFilename match {
      case this.seqIndexMetaName =>
        // TODO add metadata to metadata of filename
      case variantCallFile =>
        hasMetaExtension(filename) match {
          case true => {
            //TODO add metadata to filename
          }
          case false => {

            //TODO add sample data to filename
          }
        }
    }
    writer.close()
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
    // recognize file kind
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
        // then it's a variant call archive
        // extract package
        val archivePath = downloadDirPath+trueFilename
        val VCFFilePath = downloadDirPath+removeExtension(trueFilename)
        logger.info("EXTRACTING "+trueFilename)
        if(!Unzipper.unGzipIt(archivePath, VCFFilePath)) {  // replace existing file. Fails if directory exists with same name
          logger.error("EXTRACTION FAILED. THE PACKAGE IS PROBABLY DAMAGED OR INCOMPLETE. " +
            "SKIP TRANSFORMATION FOR THIS PACKAGE.")
            List.empty[String]
        }
        // read sample names and output resulting filenames:
        val sampleFiles = VCFInfo.biosamples(VCFFilePath).map(sample => s"$sample.gdm")
        val metadataFiles = sampleFiles.map(sample => s"$sample.gdm.meta")
        sampleFiles:::metadataFiles
    }
  }

  ///////////////////////////////   HELPER METHODS    ///////////////////////////////////////

  def removeCopyNumber(filename: String): String = {
    filename.replaceFirst("_\\d\\.", ".")
  }

  def removeExtension(filename: String): String ={
    filename.substring(0, filename.lastIndexOf("."))
  }

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

  def hasMetaExtension(filename: String): Boolean ={
    filename.endsWith(".meta")
  }

}
