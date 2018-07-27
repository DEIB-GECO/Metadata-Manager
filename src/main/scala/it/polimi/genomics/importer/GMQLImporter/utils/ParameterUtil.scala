package it.polimi.genomics.importer.GMQLImporter.utils

import it.polimi.genomics.importer.GMQLImporter.GMQLDataset

object ParameterUtil {

  var gcmConfigFile = ""

  /**
    * get parameter defined in dataset level
    *
    * @param dataset GMQLDataset definition
    * @param key     key to search in the paramter
    * @return the value of the key as scala Option
    */
  def getDatasetParameter(dataset: GMQLDataset, key: String): Option[String] = {
    dataset.parameters.find(_._1 == key).map(_._2)
  }
//  burada liste olabilir

  /**
    * get parameter defined in source level
    *
    * @param dataset GMQLDataset definition
    * @param key     key to search in the paramter
    * @return the value of the key as scala Option
    */
  def getSourceParameter(dataset: GMQLDataset, key: String): Option[String] = {
    dataset.source.parameters.find(_._1 == key).map(_._2)
  }

  /**
    * get parameter if defined first in dataset level, then source level
    *
    * @param dataset GMQLDataset definition
    * @param key     key to search in the paramter
    * @return the value of the key as scala Option
    */
  def getParameter(dataset: GMQLDataset, key: String): Option[String] = {
    getDatasetParameter(dataset, key) match {
      case None => getSourceParameter(dataset, key)
      case x => x
    }
  }

}
