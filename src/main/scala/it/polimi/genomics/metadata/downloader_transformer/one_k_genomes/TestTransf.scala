package it.polimi.genomics.metadata.downloader_transformer.one_k_genomes

import it.polimi.genomics.metadata.downloader_transformer.Transformer
import it.polimi.genomics.metadata.step.xml.{Dataset, Source}

/**
 * Created by Tom on ott, 2019
 */
class TestTransf extends Transformer {
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
    println("called with origin "+originalFilename+" and target "+filename)
    false
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
    println("candidate name: "+filename)
    if(filename == "current_0.tree")
      List("a.gdm.meta", "b.gdm.meta")
    else
      List("a.gdm", "b.gdm")
  }
}
