package it.polimi.genomics.importer.GMQLImporter.utils

import java.io.{File, PrintWriter}

import it.polimi.genomics.importer.GMQLImporter.GMQLDataset

import scala.io.Source

object DatasetNameUtil {

  def datasetFile(dataset: GMQLDataset) = dataset.fullDatasetOutputFolder + File.separator + "dataset_name.txt"

  def saveDatasetName(dataset: GMQLDataset) = {
    val datasetFileName = datasetFile(dataset)

    val datasetName =
      if (dataset.parameters.exists(_._1 == "loading_name"))
        dataset.parameters.filter(_._1 == "loading_name").head._2
      else
        dataset.source.name + "_" + dataset.name

    new PrintWriter(datasetFileName) {
      write(datasetName)
      close
    }
  }

  def loadDatasetName(dataset: GMQLDataset): String = {
    val datasetFileName = datasetFile(dataset)
    Source.fromFile(datasetFileName).mkString
  }

}
