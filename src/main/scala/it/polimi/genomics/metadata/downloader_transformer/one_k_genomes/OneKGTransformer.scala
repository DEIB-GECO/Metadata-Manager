package it.polimi.genomics.metadata.downloader_transformer.one_k_genomes

import java.io.{BufferedReader, Reader}
import java.nio.file.{Files, OpenOption, Paths, StandardCopyOption, StandardOpenOption}

import it.polimi.genomics.metadata.downloader_transformer.Transformer
import it.polimi.genomics.metadata.downloader_transformer.one_k_genomes.VCFInfo.VariantPattern
import it.polimi.genomics.metadata.step.xml.{Dataset, Source}
import it.polimi.genomics.metadata.util.{FileUtil, PatternMatch}
import org.apache.commons.cli.MissingArgumentException
import org.slf4j.{Logger, LoggerFactory}

/**
 * Created by Tom on ott, 2019
 */
class OneKGTransformer extends Transformer {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * recieves .json and .bed.gz files and transform them to get metadata in .meta files and region in .bed files.
   *
   * @param source           source where the files belong to.
   * @param originPath       path for the  "Downloads" folder
   * @param destinationPath  path for the "Transformations" folder
   * @param originalFilename name of the original file .json/.gz
   * @param filename         name of the new file .meta/.bed
   * @return List(fileId, filename) for the transformed files.
   */
  override def transform(source: Source, originPath: String, destinationPath: String, originalFilename: String, filename: String): Boolean = {
    println("TRANSFORM CALLED FOR ORIGINAL FILE "+originalFilename+" WITH EXPECTED OUTPUT "+filename)
    // create the target file
    if(filename=="a.gdm" || filename=="a.gdm.meta") {
      val path = Paths.get(destinationPath, filename)
      Files.deleteIfExists(path)
      Files.createFile(path)
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
    val populationMetaName = FileUtil.getFileNameFromPath(dataset.getParameter("population_file_path").getOrElse(
      throw new MissingArgumentException("MANDATORY PARAMETER population_file_path NOT FOUND IN XML CONFIG FILE")))
    val seqIndexMetaName = FileUtil.getFileNameFromPath(dataset.getParameter("sequence_index_file_path").getOrElse(
      throw new MissingArgumentException("MANDATORY PARAMETER sequence_index_file_path NOT FOUND IN XML CONFIG FILE")))
    val treeFileName =  FileUtil.getFileNameFromPath(dataset.getParameter("tree_file_url").getOrElse(
      throw new MissingArgumentException("MANDATORY PARAMETER tree_file_url NOT FOUND IN XML CONFIG FILE")))

    val result = if(!filename.contains("ch"))
      List.empty[String]
    else List("a.gdm", "a.gdm.meta")
    println("CANDIDATE NAMES FOR "+filename+": "+result)
    result
  }

}
