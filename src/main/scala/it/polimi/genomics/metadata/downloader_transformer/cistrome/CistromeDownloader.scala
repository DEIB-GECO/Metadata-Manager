package it.polimi.genomics.metadata.downloader_transformer.cistrome

import java.io.File

import it.polimi.genomics.metadata.downloader_transformer.Downloader
import it.polimi.genomics.metadata.downloader_transformer.database.{FileDatabase, Stage}
import it.polimi.genomics.metadata.downloader_transformer
import it.polimi.genomics.metadata.step.xml.Source
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
/**
  * Created by nachon on 7/28/17.
  */
class CistromeDownloader extends Downloader{
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


  override def download(source: Source, parallelExecution: Boolean): Unit = {
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
  override def downloadFailedFiles(source: Source, parallelExecution: Boolean): Unit = {

  }
}

