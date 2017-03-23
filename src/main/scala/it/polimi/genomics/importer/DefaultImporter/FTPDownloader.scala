package it.polimi.genomics.importer.DefaultImporter

import java.io.File

import com.google.common.hash.Hashing
import com.google.common.io.Files
import it.polimi.genomics.importer.DefaultImporter.utils.FTP
import it.polimi.genomics.importer.FileDatabase.{FileDatabase, STAGE}
import it.polimi.genomics.importer.GMQLImporter.{GMQLDownloader, GMQLSource}
import org.slf4j.LoggerFactory

import scala.io.Source

/**
  * Created by Nacho on 10/13/16.
  */
class FTPDownloader extends GMQLDownloader {
  val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * downloads the files from the source defined in the information
    * into the folder defined in the source
    *
    * @param source contains specific download and sorting info.
    */
  override def download(source: GMQLSource): Unit = {
    if(source.downloadEnabled) {
      logger.info("Starting download for: " + source.name)
      if (!new java.io.File(source.outputFolder).exists) {
        new java.io.File(source.outputFolder).mkdirs()
      }
      val ftp = new FTP()

      //the mark to compare is done here because the iteration on ftp is based on ftp folders and not
      //on the source datasets.
      val sourceId = FileDatabase.sourceId(source.name)
      source.datasets.foreach(dataset => {
        if (dataset.downloadEnabled) {
          val datasetId = FileDatabase.datasetId(sourceId,dataset.name)
          FileDatabase.markToCompare(datasetId,STAGE.DOWNLOAD)
        }
      })

      logger.debug("trying to connect to FTP: " + source.url +
        " - username: " + source.parameters.filter(_._1 == "username").head._2 +
        " - password: " + source.parameters.filter(_._1 == "password").head._2)
      if (ftp.connectWithAuth(
        source.url,
        source.parameters.filter(_._1 == "username").head._2,
        source.parameters.filter(_._1 == "password").head._2).getOrElse(false)) {

        logger.info("Connected to ftp: " + source.url)
        val workingDirectory = ftp.workingDirectory()
        ftp.disconnect()
        recursiveDownload(workingDirectory, source)

        source.datasets.foreach(dataset => {
          if (dataset.downloadEnabled) {
            val datasetId = FileDatabase.datasetId(sourceId,dataset.name)
            FileDatabase.markAsOutdated(datasetId,STAGE.DOWNLOAD)
          }
        })
        logger.info(s"Download for ${source.name} Finished.")
      }
      else
        logger.warn("ftp connection with " + source.url + " couldn't be handled.")
    }
  }

  /**
    * recursively checks all folders and subfolders matching with the regular expressions defined in the information
    *
    * @param workingDirectory    current folder of the ftp connection
    * @param source configuration for the downloader, folders for input and output by regex and also for files.
    */
  private def recursiveDownload(workingDirectory: String, source: GMQLSource): Unit = {
    checkFolderForDownloads(workingDirectory, source)
    downloadSubfolders(workingDirectory, source)
  }

  /**
    * given a folder, searches all the possible links to download and downloads if signaled by Updater and information
    * puts all content into information.outputFolder/dataset.outputFolder/Downloads/
    * (this is because TCGA2BED does no transform and we dont want just to copy the files).
    *
    * @param workingDirectory    current state of the ftp connection
    * @param source configuration for downloader, folders for input and output by regex and also for files
    */
  private def checkFolderForDownloads(workingDirectory: String, source: GMQLSource): Unit = {
    val sourceId = FileDatabase.sourceId(source.name)
    for (dataset <- source.datasets) {
      if (dataset.downloadEnabled) {
        val datasetId = FileDatabase.datasetId(sourceId, dataset.name)
        if (workingDirectory.matches(dataset.parameters.filter(_._1 == "folder_regex").head._2)) {
          val outputPath = source.outputFolder + File.separator + dataset.outputFolder + File.separator + "Downloads"

          logger.info("Searching: " + workingDirectory)
          if (!new java.io.File(outputPath).exists) {
            try {
              new java.io.File(outputPath).mkdirs()
            }
            catch {
              case ex: Exception => logger.error(s"could not create the folder $outputPath")
            }
          }
          val ftpFiles = new FTP
          if (ftpFiles.connectWithAuth(
            source.url,
            source.parameters.filter(_._1 == "username").head._2,
            source.parameters.filter(_._1 == "password").head._2).getOrElse(false)) {
            ftpFiles.cd(workingDirectory)

            val unfilteredFiles = ftpFiles.listFiles()
            val files = unfilteredFiles.filter(_.isFile).filter(_.getName.matches(
              dataset.parameters.filter(_._1 == "files_regex").head._2))
            ftpFiles.disconnect()
            var md5Downloaded = false
            if (dataset.parameters.exists(_._1 == "md5_checksum_tcga2bed")) {
              val aux2 = dataset.parameters.filter(_._1 == "md5_checksum_tcga2bed")
              val aux1 = aux2.head._2
              val file = unfilteredFiles.filter(_.getName == aux1).head
              val url = workingDirectory + File.separator + file.getName
              val fileId = FileDatabase.fileId(datasetId, url, STAGE.DOWNLOAD, file.getName)
              FileDatabase.checkIfUpdateFile(
                fileId,
                "",
                file.getSize.toString,
                file.getTimestamp.getTime.toString)
                val nameAndCopyNumber: (String, Int) = FileDatabase.getFileNameAndCopyNumber(fileId)
                val name =
                  if (nameAndCopyNumber._2 == 1) nameAndCopyNumber._1
                  else nameAndCopyNumber._1.replaceFirst("\\.", "_" + nameAndCopyNumber._2 + ".")
                val outputUrl = outputPath + File.separator + name
                val threadDownload = new Thread {
                  override def run(): Unit = {
                    try {
                      var downloaded = false
                      var timesTried = 0
                      while (!downloaded && timesTried < 6) {
                        val ftpDownload = new FTP()
                        val connected = ftpDownload.connectWithAuth(
                          source.url,
                          source.parameters.filter(_._1 == "username").head._2,
                          source.parameters.filter(_._1 == "password").head._2).getOrElse(false)
                        if (connected) {
                          logger.info("Downloading: " + url)
                          if (ftpDownload.cd(workingDirectory).getOrElse(false))
                            downloaded = ftpDownload.downloadFile(file.getName, outputUrl).getOrElse(false)
                          else
                            logger.error(s"couldn't access directory $workingDirectory")
                        }
                        else
                          logger.error(s"couldn't connect to ${source.url}")
                        timesTried += 1
                        if (ftpDownload.connected)
                          ftpDownload.disconnect()
                      }
                      if (!downloaded) {
                        logger.error("Downloading: " + url + " FAILED")
                        FileDatabase.markAsFailed(fileId)
                      }
                      else {
                        md5Downloaded = true
                        logger.info("Downloading: " + url + " DONE")
                        //here I have to get the hash and update it for the meta and the data files.
                        //so I wait to get the meta file and then I mark the data file to updated
                        val hash = Files.hash(new File(outputUrl), Hashing.md5()).toString
                        //get the hash, I will put the same on both files.
                        FileDatabase.markAsUpdated(fileId, new File(outputUrl).length.toString, hash)
                      }
                    }
                    catch {
                      case ex: InterruptedException => logger.error(s"Download of $url took too long, aborted by timeout")
                      case ex: Exception => logger.error("Could not connect to the FTP server: " + ex.getMessage)
                    }
                  }
                }
                threadDownload.start()
                try {
                  threadDownload.join(10 * 60 * 1000)
                }
                catch {
                  case ex: InterruptedException => logger.error(s"Download of $url was interrupted")
                }

            }
            for (file <- files) {

              val url = workingDirectory + File.separator + file.getName
              val fileId = FileDatabase.fileId(datasetId, url, STAGE.DOWNLOAD, file.getName)
              val hash =
                if (dataset.parameters.exists(_._1 == "md5_checksum_tcga2bed") && md5Downloaded) {
                  val filename = dataset.parameters.filter(_._1 == "md5_checksum_tcga2bed").head._2
                  val md5File = Source.fromFile(outputPath + File.separator + filename)
                  val lines = md5File.getLines().filterNot(_ == "")
                  //                  if (lines.exists(_.split('\t').head == file.getName)) {
                  lines.filter(_.split('\t').head == file.getName).map(line => {
                    val hashCleaned = line.split('\t')
                    val hashCleanedDropped = hashCleaned.drop(1)
                    val hashAlone = hashCleanedDropped.head
                    hashAlone
                  }).next()
                  //                  }
                  //                  else{
                  //                    ""
                  //                  }
                }
                else
                  ""
              if (FileDatabase.checkIfUpdateFile(
                //I have to get the hash from the .meta file.
                fileId,
                hash,
                file.getSize.toString,
                file.getTimestamp.getTime.toString)) {
                val nameAndCopyNumber: (String, Int) = FileDatabase.getFileNameAndCopyNumber(fileId)
                val name =
                  if (nameAndCopyNumber._2 == 1) nameAndCopyNumber._1
                  else nameAndCopyNumber._1.replaceFirst("\\.", "_" + nameAndCopyNumber._2 + ".")
                val outputUrl = outputPath + File.separator + name
                val threadDownload = new Thread {
                  override def run(): Unit = {
                    try {
                      var downloaded = false
                      var timesTried = 0
                      while (!downloaded && timesTried < 4) {
                        val ftpDownload = new FTP()
                        val connected = ftpDownload.connectWithAuth(
                          source.url,
                          source.parameters.filter(_._1 == "username").head._2,
                          source.parameters.filter(_._1 == "password").head._2).getOrElse(false)
                        if (connected) {
                          logger.info("Downloading: " + url)
                          if (ftpDownload.cd(workingDirectory).getOrElse(false)) {
                            downloaded = ftpDownload.downloadFile(file.getName, outputUrl).getOrElse(false)

                            val fileToHash = new File(outputUrl)
                            val hashToCompare = Files.hash(fileToHash, Hashing.md5()).toString
                            if (hashToCompare != hash) {
                              downloaded = false
                              logger.error(s"file ${file.getName} download is corrupted, trying again.")
                            }
                          }
                          else
                            logger.error(s"couldn't access directory $workingDirectory")
                        }
                        else
                          logger.error(s"couldn't connect to ${source.url}")
                        timesTried += 1
                        if (ftpDownload.connected)
                          ftpDownload.disconnect()
                      }
                      if (!downloaded) {
                        logger.error("Downloading: " + url + " FAILED")
                        FileDatabase.markAsFailed(fileId)
                      }
                      else {
                        logger.info("Downloading: " + url + " DONE")
                        //here I have to get the hash and update it for the meta and the data files.
                        //so I wait to get the meta file and then I mark the data file to updated
                        val hash = Files.hash(new File(outputUrl), Hashing.md5()).toString
                        //get the hash, I will put the same on both files.
                        FileDatabase.markAsUpdated(fileId, new File(outputUrl).length.toString, hash)
                      }
                    }
                    catch {
                      case ex: InterruptedException => logger.error(s"Download of $url took too long, aborted by timeout")
                      case ex: Exception => logger.error("Could not connect to the FTP server: " + ex.getMessage)
                    }
                  }
                }
                threadDownload.start()
                try {
                  threadDownload.join(10 * 60 * 1000)
                }
                catch {
                  case ex: InterruptedException => logger.error(s"Download of $url was interrupted")
                }
              }
            }
          }
          else
            logger.error("connection lost with FTP, skipping " + workingDirectory)
        }
      }
    }
  }


  /**
    * Finds all subfolders in the working directory and performs checkFolderForDownloads on it
    *
    * @param workingDirectory    current folder of the ftp connection
    * @param source configuration for downloader, folders for input and output by regex and also for files
    */
  private def downloadSubfolders(workingDirectory: String, source: GMQLSource): Unit = {

    val ftp = new FTP()
    if (ftp.connectWithAuth(
      source.url,
      source.parameters.filter(_._1 == "username").head._2,
      source.parameters.filter(_._1 == "password").head._2).getOrElse(false)) {
      if(ftp.cd(workingDirectory).getOrElse(false)) {
        logger.info("working directory: " + workingDirectory)
        val directories = ftp.listDirectories()
        ftp.disconnect()
        directories.foreach(directory =>
          recursiveDownload(
            if (workingDirectory.endsWith(File.separator)) workingDirectory + directory.getName
            else workingDirectory + File.separator + directory.getName,
            source))
      }
    }
    else
      logger.error("connection lost with FTP, skipped " + workingDirectory)
  }
}
