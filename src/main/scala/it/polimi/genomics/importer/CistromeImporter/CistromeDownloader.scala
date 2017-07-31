package it.polimi.genomics.importer.CistromeImporter

import it.polimi.genomics.importer.GMQLImporter.{GMQLDownloader, GMQLSource}
import java.io.File

import it.polimi.genomics.importer.FileDatabase.{FileDatabase, STAGE}
import it.polimi.genomics.importer.GMQLImporter.{GMQLDownloader, GMQLSource}
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
/**
  * Created by nachon on 7/28/17.
  */
class CistromeDownloader extends GMQLDownloader{
  val logger = LoggerFactory.getLogger( this.getClass )

  /**
    * downloads the files from the source defined in the loader
    * into the folder defined in the loader
    *
    * For each dataset, download method should put the downloaded files inside
    * /source.outputFolder/dataset.outputFolder/Downloads
    *
    * @param source contains specific download and sorting info.
    */


  override def download(source: GMQLSource, parallelExecution: Boolean): Unit = {
  }

  /**
    * downloads the failed files from the source defined in the loader
    * into the folder defined in the loader
    *
    * For each dataset, download method should put the downloaded files inside
    * /source.outputFolder/dataset.outputFolder/Downloads
    *
    * @param source            contains specific download and sorting info.
    * @param parallelExecution defines parallel or sequential execution
    */
  override def downloadFailedFiles(source: GMQLSource, parallelExecution: Boolean): Unit = {

  }
}

