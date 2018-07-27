package it.polimi.genomics.metadata.step.utils

import java.io.File

import it.polimi.genomics.metadata.step.xml.Dataset

object DirectoryNamingUtil {

  val downloadFolderName = "Downloads"
  val transformFolderName = "Transformations"
  val cleanFolderName = "Cleaned"
  val flattenFolderName = "Flattened"

  def getDownloadedDirectory(dataset: Dataset): File = {
    val datasetOutputFolder = dataset.fullDatasetOutputFolder
    new File(datasetOutputFolder, downloadFolderName)
  }

  def getTransformedDirectory(dataset: Dataset): File = {
    val datasetOutputFolder = dataset.fullDatasetOutputFolder
    new File(datasetOutputFolder, transformFolderName)
  }


  def getCleanedDirectory(dataset: Dataset): File = {
    val datasetOutputFolder = dataset.fullDatasetOutputFolder
    new File(datasetOutputFolder, cleanFolderName)
  }

  def getFlattenedDirectory(dataset: Dataset): File = {
    val datasetOutputFolder = dataset.fullDatasetOutputFolder
    new File(datasetOutputFolder, flattenFolderName)
  }

}
