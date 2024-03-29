package it.polimi.genomics.metadata.downloader_transformer

import it.polimi.genomics.metadata.step.xml.{Dataset, Source}

/**
  * Created by Nacho on 10/13/16.
  * once the files are downloaded, sorter using a loader
  * will check how to sort and insert them into the GMQLRepository
  * every sorter uses specific loader
  * ex: TCGA2BEDSorter should use TCGA2BEDSorter
  */
trait Transformer {

  /**
   * Callback received once before any transformation on this dataset occurs (by means of method transform)
   * @param dataset the dataset to transform
   */
  def onBeforeTransformation(dataset: Dataset):Unit ={
    // stub method
  }

  /**
    * recieves .json and .bed.gz files and transform them to get metadata in .meta files and region in .bed files.
    * @param source source where the files belong to.
    * @param originPath path for the  "Downloads" folder
    * @param destinationPath path for the "Transformations" folder
    * @param originalFilename name of the original file .json/.gz
    * @param filename name of the new file .meta/.bed
    * @return List(fileId, filename) for the transformed files.
    */
  def transform(source: Source, originPath: String, destinationPath: String, originalFilename:String,
                filename: String):Boolean

  /**
   * Callback received once after all transformations have applied.
   * If the xml parameter many_to_many_transformation is true, this callback is invoked between all transformations and
   * the post-processing operations executed by TransformerStep. If false instead, it's invoked after all the files have
   * been transformed and post-processed.
   * @param dataset the transformed dataset.
   */
  def onAllTransformationsDone(dataset: Dataset, priorPostprocessing:Boolean):Unit ={
    // stub method
  }

  /**
    * by receiving an original filename returns the new GDM candidate name(s).
    * The region file must be before than related meta file
    * @param filename original filename
    * @param dataset dataset where the file belongs to
    * @param source source where the files belong to.
    * @return candidate names for the files derived from the original filename.
    */
    def getCandidateNames(filename: String, dataset :Dataset, source: Source): List[String]
}
