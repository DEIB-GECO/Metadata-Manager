package it.polimi.genomics.importer.GMQLImporter

/**
  * Created by Nacho on 10/13/16.
  * once the files are downloaded, sorter using a loader
  * will check how to sort and insert them into the GMQLRepository
  * every sorter uses specific loader
  * ex: TCGA2BEDSorter should use TCGA2BEDSorter
  */
trait GMQLTransformer {

  /**
    * recieves .json and .bed.gz files and transform them to get metadata in .meta files and region in .bed files.
    * @param source source where the files belong to.
    * @param originPath path for the  "Downloads" folder
    * @param destinationPath path for the "Transformations" folder
    * @param originalFilename name of the original file .json/.gz
    * @param filename name of the new file .meta/.bed
    * @return List(fileId, filename) for the transformed files.
    */
  def transform(source: GMQLSource,originPath: String, destinationPath: String, originalFilename:String,
                filename: String):Boolean

  /**
    * by receiving an original filename returns the new GDM candidate name(s).
    * @param filename original filename
    * @param dataset dataset where the file belongs to
    * @param source source where the files belong to.
    * @return candidate names for the files derived from the original filename.
    */
    def getCandidateNames(filename: String, dataset :GMQLDataset, source: GMQLSource): List[String]
}
