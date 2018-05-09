package it.polimi.genomics.importer.CistromeImporter

import it.polimi.genomics.importer.GMQLImporter.{GMQLDownloader, GMQLSource}
import java.io.File
import java.net.URL

import it.polimi.genomics.importer.FileDatabase.{FileDatabase, STAGE}
import it.polimi.genomics.importer.GMQLImporter.{GMQLDownloader, GMQLSource}
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import scala.util.Try




import java.io.File
import java.net.URL
import java.nio.file.Paths

import it.polimi.genomics.importer.DefaultImporter.utils.{OAuth, csvDownload}
import it.polimi.genomics.importer.FileDatabase.{FileDatabase, STAGE}
import it.polimi.genomics.importer.GMQLImporter.{GMQLDownloader, GMQLSource}
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.slf4j.{Logger, LoggerFactory}

import scala.language.postfixOps
import scala.sys.process._
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._

/**
  * Created by nachon on 7/28/17.
  */
class CistromeDownloader extends GMQLDownloader{
  val logger = LoggerFactory.getLogger( this.getClass )

  /**
    * checks if the given URL exists
    *
    * @param path URL to check
    * @return URL exists
    */
  def urlExists(path: String): Boolean = {
    try {
      scala.io.Source.fromURL(path)
      true
    } catch {
      case _: Throwable => false
    }
  }

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
    val sourceId = FileDatabase.sourceId(source.name)


    //start the iteration for every dataset.
    for (dataset <- source.datasets) {
      if (dataset.downloadEnabled) {

        var totalFiles = 0
        var downloadedFiles = 0



        val datasetId = FileDatabase.datasetId(sourceId, dataset.name)

        val path = dataset.parameters.filter(_._1.toLowerCase == "file_full_path").head._2

        val name: String = path.split("/").last


        val urlSource = scala.io.Source.fromURL(path)
        totalFiles += 1

        val fileId = FileDatabase.fileId(datasetId, path, STAGE.DOWNLOAD, name)

        val outputPath = source.outputFolder + File.separator + dataset.outputFolder + File.separator + "Downloads"
        if (!new java.io.File(outputPath).exists) {
          try {
            new java.io.File(outputPath).mkdirs()
            logger.debug(s"$outputPath created")
          }
          catch {
            case _: Exception => logger.warn(s"could not create the folder $outputPath")
          }
        }
        if (!new java.io.File(outputPath).exists) {
          new java.io.File(outputPath).mkdirs()
        }

        val downloadOutcome = downloadFileFromURL(path, outputPath + File.separator + name)
        if (downloadOutcome.isSuccess) {
          logger.info("Downloading: " + name + " from: " + path + name + " DONE")
          downloadedFiles += 1
          FileDatabase.markAsUpdated(fileId, new File(outputPath + File.separator + name).length.toString)
        }
        else {
          logger.error("Downloading: " + name + " from: " + path + name + " FAILED ")
          FileDatabase.markAsFailed(fileId)
        }
      }
    }



  }

  def downloadFileFromURL(url: String, path: String): Try[Unit] = Try(new URL(url) #> new File(path) !!)


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

