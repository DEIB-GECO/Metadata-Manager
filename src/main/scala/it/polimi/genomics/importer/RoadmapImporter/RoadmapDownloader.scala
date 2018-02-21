package it.polimi.genomics.importer.RoadmapImporter

import java.io.File
import java.net.URL

import it.polimi.genomics.importer.DefaultImporter.utils.{OAuth, csvDownload}
import it.polimi.genomics.importer.FileDatabase.{FileDatabase, STAGE}
import it.polimi.genomics.importer.GMQLImporter.{GMQLDownloader, GMQLSource}
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.language.postfixOps
import scala.sys.process._
import scala.util.{Failure, Success, Try}

/**
  * download file available through HTTP Roadmap Epigenomics repository
  */
class RoadmapDownloader extends GMQLDownloader {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

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
    * given a url and destination path, downloads that file into the path
    *
    * @param url  source file url.
    * @param path destination file path and name.
    */
  def downloadFileFromURL(url: String, path: String): Try[Unit] = Try(new URL(url) #> new File(path) !!)

  /**
    * Download metadata in a Google spreadsheet as csv files
    *
    * @param source configuration for the downloader, folders for input and output by regex and also for files.
    */
  def downloadMetadata(source: GMQLSource): Unit = {
    //authentication to gain access to Google Spreadsheet API
    val authentication: OAuth = new OAuth
    val service = authentication.getSheetsService()

    val sourceId = FileDatabase.sourceId(source.name)

    if (source.downloadEnabled) {
      //for each dataset download the metadata csv
      source.datasets.foreach(dataset => {
        if (dataset.downloadEnabled) {
          //extracting spreadsheetID provided through config file
          val spreadsheetUrl: String = dataset.parameters.filter(_._1.toLowerCase == "spreadsheet_url").head._2
          val spreadsheetId: String = "/d/.*/".r.findFirstIn(spreadsheetUrl).get.split("/")(2)
          //connect to spreadsheet through API
          val spreadsheet = service.spreadsheets().get(spreadsheetId).execute()

          logger.info("Starting metadata download for: " + source.name)
          val outputPath = source.outputFolder  + File.separator + dataset.outputFolder + File.separator + "Downloads"
          //check existence of the directory
          if (!new java.io.File(outputPath).exists) {
            try {
              new java.io.File(outputPath).mkdirs()
              logger.debug(s"$outputPath created")
            }
            catch {
              case ex: Exception => logger.warn(s"could not create the folder $outputPath")
            }
          }

          //download all the sheet (not hidden) as csv
          val spreadsheetDownloader = new csvDownload(spreadsheetId, spreadsheet.getProperties.getTitle)
          val sheetsList = spreadsheet.getSheets
          sheetsList.asScala.toList.foreach(sh =>
            if (sh.getProperties.getHidden == null) {
              val csvDwnldOutcome = Try(spreadsheetDownloader.get(sh.getProperties.getSheetId.toString, outputPath, sh.getProperties.getTitle))
              csvDwnldOutcome match {
                case Success(_) => logger.info(s"${sh.getProperties.getTitle} downloaded from ${spreadsheet.getProperties.getTitle} with success")
                case Failure(_) => logger.warn(s"failed to download ${sh.getProperties.getTitle} from ${spreadsheet.getProperties.getTitle}")
              }
            })
        }
      })
    }
  }
  /**
    * downloads the files from the source defined in the information
    * into the folder defined in the source and its dataset
    *
    * @param source configuration for the downloader, folders for input and output by regex and also for files.
    */
  override def download(source: GMQLSource, parallelExecution: Boolean): Unit = {
    if (urlExists(source.url) && source.downloadEnabled) {
      logger.info("Starting download for: " + source.name)
      if (!new java.io.File(source.outputFolder).exists) {
        new java.io.File(source.outputFolder).mkdirs()
      }
      //same as FTP the mark to compare is done here because the iteration on http is based on http folders and not
      //on the source datasets.
      val sourceId = FileDatabase.sourceId(source.name)
      source.datasets.foreach(dataset => {
        if (dataset.downloadEnabled) {
          val datasetId = FileDatabase.datasetId(sourceId, dataset.name)
          val outputPath = source.outputFolder + File.separator + dataset.outputFolder + File.separator + "Downloads"
          if (!new File(outputPath).exists())
            new File(outputPath).mkdirs()
          FileDatabase.markToCompare(datasetId, STAGE.DOWNLOAD)
        }
      })

      recursiveDownload(source.url, source)
      downloadMetadata(source)

      source.datasets.foreach(dataset => {
        if (dataset.downloadEnabled) {
          val datasetId = FileDatabase.datasetId(sourceId, dataset.name)
          FileDatabase.markAsOutdated(datasetId, STAGE.DOWNLOAD)
        }
      })
      logger.info(s"Download for ${source.name} Finished.")
      downloadFailedFiles(source, parallelExecution)
    }
  }

  /**
    * recursively checks all folders and subfolders matching with the regular expressions defined in the source
    *
    * @param path   current path of the http connection
    * @param source configuration for the downloader, folders for input and output by regex and also for files.
    */
  private def recursiveDownload(path: String, source: GMQLSource): Unit = {

    if (urlExists(path)) {
      val urlSource = scala.io.Source.fromURL(path)
      val result = urlSource.mkString
      val document: Document = Jsoup.parse(result)
      checkFolderForDownloads(path, document, source)
      downloadSubFolders(path, document, source)
    }
  }

  /**
    * given a folder, searches all the possible links to download and downloads if signaled by Updater and loader
    *
    * @param path     current directory
    * @param document current location  Jsoup document
    * @param source   contains download information
    */
  def checkFolderForDownloads(path: String, document: Document, source: GMQLSource): Unit = {
    //id of the source with the name
    val sourceId = FileDatabase.sourceId(source.name)

    //start the iteration for every dataset.
    for (dataset <- source.datasets) {
      if (dataset.downloadEnabled) {
        var totalFiles = 0
        var downloadedFiles = 0

        val datasetId = FileDatabase.datasetId(sourceId, dataset.name)
        if (path.matches(dataset.parameters.filter(_._1.toLowerCase == "folder_regex").head._2)) {
          logger.info("Searching into: " + path)
          val elements = document.select("a") //all the links in the page
          val outputPath = source.outputFolder + File.separator + dataset.outputFolder + File.separator + "Downloads"
          //check existence of the directory
          if (!new java.io.File(outputPath).exists) {
            try {
              new java.io.File(outputPath).mkdirs()
              logger.debug(s"$outputPath created")
            }
            catch {
              case ex: Exception => logger.warn(s"could not create the folder $outputPath")
            }
          }
          if (!new java.io.File(outputPath).exists) {
            new java.io.File(outputPath).mkdirs()
          }
          for (i <- 0 until elements.size()) {
            //candidate name is the same as origin name.
            var candidateName: String = elements.get(i).attr("href")
            if (candidateName.startsWith("/"))
              candidateName = candidateName.substring(1)
            //if the file matches with a regex to download
            if (candidateName.matches(dataset.parameters.filter(_._1.toLowerCase == "files_regex").head._2)) {
              totalFiles += 1
              val date = elements.get(i).parent.nextElementSibling.`val`
              val size = elements.get(i).parent.nextElementSibling().nextElementSibling().`val`
              val fileId = FileDatabase.fileId(datasetId, path + candidateName, STAGE.DOWNLOAD, candidateName)
              val nameAndCopyNumber: (String, Int) = FileDatabase.getFileNameAndCopyNumber(fileId)
              val name =
                if (nameAndCopyNumber._2 == 1) nameAndCopyNumber._1
                else nameAndCopyNumber._1.replaceFirst("\\.", "_" + nameAndCopyNumber._2 + ".")

              val checkDownload = FileDatabase.checkIfUpdateFile(fileId, "no hash here", size, date)
              if (checkDownload) {
                logger.info(s"Starting download for ${path + candidateName}")
                val downloadOutcome = downloadFileFromURL(path + candidateName, outputPath + File.separator + name)
                if (downloadOutcome.isSuccess) {
                  logger.info("Downloading: " + path + candidateName + " from: " + outputPath + File.separator + name + " DONE")
                  downloadedFiles += 1
                  FileDatabase.markAsUpdated(fileId, new File(outputPath + File.separator + name).length.toString)
                }
                else {
                  logger.error("Downloading: " + path + candidateName + " from: " + outputPath + File.separator + name + " failed: ")
                  FileDatabase.markAsFailed(fileId)
                }
              }
            }
          }
          FileDatabase.runDatasetDownloadAppend(datasetId, dataset, totalFiles, downloadedFiles)
          if (totalFiles == downloadedFiles) {
            //add successful message to database, have to sum up all the dataset's folders.
            logger.info(s"All $totalFiles files for folder $path of dataset ${dataset.name} of source ${source.name} downloaded correctly.")
          }
          else {
            //add the warning message to the database
            logger.warn(s"Dataset ${dataset.name} of source ${source.name} downloaded $downloadedFiles/$totalFiles files.")
          }
        }
      }
    }
  }

  /**
    * Finds all subfolders in the working directory and performs checkFolderForDownloads on it
    *
    * @param path     working directory
    * @param document current location Jsoup document
    * @param source   contains download information
    */
  def downloadSubFolders(path: String, document: Document, source: GMQLSource): Unit = {
    //directories is to avoid taking backward folders
    val folders = path.split("/")
    var directories = List[String]()
    for (i <- folders) {
      if (directories.nonEmpty)
        directories = directories :+ directories.last + File.separator + i
      else
        directories = directories :+ i
    }
    val elements = document.select("a")

    for (i <- 0 until elements.size()) {

      var url = elements.get(i).attr("href")

      if (url.endsWith("/") && !url.contains(".." + "/") && !directories.contains(url)) {
        if (url.startsWith("/"))
          url = url.substring(1)
        recursiveDownload(path + url, source)
      }
    }
  }

  /**
    * downloads the failed files from the source defined in the loader
    * into the folder defined in the loader
    *
    * For each dataset, download method should put the downloaded files inside
    * /source.outputFolder/dataset.outputFolder/Downloads
    *
    * @param source contains specific download and sorting info.
    */
  override def downloadFailedFiles(source: GMQLSource, parallelExecution: Boolean): Unit = {
    logger.info(s"Downloading failed files for source ${source.name}")
    val sourceId = FileDatabase.sourceId(source.name)
    val downloadThreads = source.datasets.map(dataset => {
      new Thread {
        override def run(): Unit = {
          var downloadedFiles = 0
          var counter = 0
          logger.info(s"Downloading failed files for dataset ${dataset.name}")
          val datasetId = FileDatabase.datasetId(sourceId, dataset.name)
          val failedFiles = FileDatabase.getFailedFiles(datasetId, STAGE.DOWNLOAD)
          val totalFiles = failedFiles.size
          //in file I have (fileId,name,copyNumber,url, hash)
          failedFiles.foreach(file => {
            val url = file._4
            val fileId = file._1
            val filename =
              if (file._3 == 1) file._2
              else file._2.replaceFirst("\\.", "_" + file._3 + ".")
            val filePath =
              source.outputFolder + File.separator + dataset.outputFolder +
                File.separator + "Downloads" + File.separator + filename
            counter = counter + 1
            val downloadOutcome = downloadFileFromURL(url , filePath)
            if (downloadOutcome.isSuccess) {
              logger.info("Downloading: " + url + " from: " + filePath + " DONE")
              downloadedFiles += 1
              FileDatabase.markAsUpdated(fileId, new File(filePath).length.toString)
            }
            else {
              logger.error("Downloading: " + url + " from: " + filePath + " failed: ")
              FileDatabase.markAsFailed(fileId)
            }
          })
          FileDatabase.runDatasetDownloadAppend(datasetId, dataset, 0, downloadedFiles)
        }
      }
    })
    if(parallelExecution) {
      downloadThreads.foreach(_.start())
      downloadThreads.foreach(_.join())
    }
    else{
      for(thread <- downloadThreads){
        thread.start()
        thread.join()
      }
    }
  }

}
