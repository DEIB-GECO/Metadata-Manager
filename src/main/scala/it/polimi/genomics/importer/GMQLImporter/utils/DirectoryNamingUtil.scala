package it.polimi.genomics.importer.GMQLImporter.utils

import java.io.File

import it.polimi.genomics.importer.GMQLImporter.GMQLDataset

object DirectoryNamingUtil {

  val downloadFolderName = "Downloads"
  val transformFolderName = "Transformations"
  val cleanFolderName = "Cleaned"
  val flattenFolderName = "Flattened"

  def getDownloadedDirectory(dataset: GMQLDataset): File = {
    val datasetOutputFolder = dataset.fullDatasetOutputFolder
    new File(datasetOutputFolder, downloadFolderName)
  }

  def getTransformedDirectory(dataset: GMQLDataset): File = {
    val datasetOutputFolder = dataset.fullDatasetOutputFolder
    new File(datasetOutputFolder, transformFolderName)
  }


  def getCleanedDirectory(dataset: GMQLDataset): File = {
    val datasetOutputFolder = dataset.fullDatasetOutputFolder
    new File(datasetOutputFolder, cleanFolderName)
  }

  def getFlattenedDirectory(dataset: GMQLDataset): File = {
    val datasetOutputFolder = dataset.fullDatasetOutputFolder
    new File(datasetOutputFolder, flattenFolderName)
  }

}
