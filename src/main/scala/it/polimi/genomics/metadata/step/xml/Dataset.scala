package it.polimi.genomics.metadata.step.xml

import java.io.File

import it.polimi.genomics.metadata.step.utils.{ParameterUtil, SchemaLocation}

/**
  * Created by Nacho on 10/17/16.
  */
/**
  * represents a dataset from a source
  *
  * @param outputFolder     working subdirectory for the dataset
  * @param schemaUrl        .schema file location
  * @param schemaLocation   indicates if the schema is on local or remote location
  * @param downloadEnabled  indicates whether download or not the datasets.
  * @param transformEnabled indicates whether transform or not the datasets.
  * @param loadEnabled      indicates whether load or not the datasets.
  * @param parameters       list with parameters
  */
case class Dataset(
                    name: String,
                    outputFolder: String,
                    schemaUrl: String,
                    schemaLocation: SchemaLocation.Value,
                    downloadEnabled: Boolean,
                    transformEnabled: Boolean,
                    loadEnabled: Boolean,
                    parameters: Seq[(String, String, String, String)],
                    var source: Source = null
                  ) {
  //TODO change this anyplace that calculates output folder
  def fullDatasetOutputFolder = source.outputFolder + File.separator + this.outputFolder

  def getParameter(key: String): Option[String] = ParameterUtil.getParameter(this, key)

  def getSourceParameter(key: String): Option[String] = ParameterUtil.getSourceParameter(this, key)

  def getDatasetParameter(key: String): Option[String] = ParameterUtil.getDatasetParameter(this, key)

}
