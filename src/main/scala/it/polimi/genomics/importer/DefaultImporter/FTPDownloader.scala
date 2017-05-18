package it.polimi.genomics.importer.DefaultImporter

import java.io.{File, FileInputStream}
import java.security.{DigestInputStream, MessageDigest}

import it.polimi.genomics.importer.DefaultImporter.utils.FTP
import it.polimi.genomics.importer.FileDatabase.{FileDatabase, STAGE}
import it.polimi.genomics.importer.GMQLImporter.{GMQLDataset, GMQLDownloader, GMQLSource}
import org.apache.commons.net.ftp.FTPFile
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
    if (source.downloadEnabled) {
      logger.info("Starting download for: " + source.name)
      if (!new java.io.File(source.outputFolder).exists) {
        new java.io.File(source.outputFolder).mkdirs()
      }

      //the mark to compare is done here because the iteration on ftp is based on ftp folders and not
      //on the source datasets.
      val sourceId = FileDatabase.sourceId(source.name)
      source.datasets.foreach(dataset => {
        if (dataset.downloadEnabled) {
          val datasetId = FileDatabase.datasetId(sourceId, dataset.name)
          FileDatabase.markToCompare(datasetId, STAGE.DOWNLOAD)
        }
      })
        val workingDirectory = getBaseWorkingDirectory(source)
        recursiveDownload(workingDirectory, source)

        source.datasets.foreach(dataset => {
          if (dataset.downloadEnabled) {
            val datasetId = FileDatabase.datasetId(sourceId, dataset.name)
            FileDatabase.markAsOutdated(datasetId, STAGE.DOWNLOAD)
          }
        })
        logger.info(s"Download for ${source.name} Finished.")
      downloadFailedFiles(source)
      }
  }

  /**
    * gets the base working directory of an ftp directory
    * @param source source to connect to ftp
    * @return base working directory
    */
  def getBaseWorkingDirectory(
                          source: GMQLSource
                        ): String = {
    var workingDirectory = ""
    var directoryOk = false
    val threadDownload = new Thread {
      override def run(): Unit = {
        try {
          var timesTried = 0
          while (timesTried < 4 && !directoryOk) {
            val ftpDownload = new FTP()
            val connected = ftpDownload.connectWithAuth(
              source.url,
              source.parameters.filter(_._1 == "username").head._2,
              source.parameters.filter(_._1 == "password").head._2).getOrElse(false)
            if (connected) {
              logger.info(s"connected to ftp ${source.name}")
              workingDirectory = ftpDownload.workingDirectory()
                directoryOk = true
                if (timesTried == 3) {
                  logger.warn("Connection lost with the FTP server, skipping")
                }
                else if (timesTried == 2) {
                  logger.info("Seems internet connection is lost, resuming in 5 minutes.")
                  Thread.sleep(1000 * 60 * 5)
                }
              timesTried += 1
              ftpDownload.disconnect()
            }
            else
              logger.error(s"couldn't connect to ${source.url}")
          }
        }
        catch {
          case ex: InterruptedException => logger.error(s"Listing files took too long, aborted by timeout")
          case ex: Exception => logger.error("Could not connect to the FTP server: " + ex.getMessage)
        }
      }
    }
    threadDownload.start()
    try {
      threadDownload.join(10 * 60 * 1000)
    }
    catch {
      case ex: InterruptedException =>
        logger.error(s"Could'nt access base FTP directory")
    }
    workingDirectory
  }
  /**
    * recursively checks all folders and subfolders matching with the regular expressions defined in the information
    *
    * @param workingDirectory current folder of the ftp connection
    * @param source           configuration for the downloader, folders for input and output by regex and also for files.
    */
  private def recursiveDownload(workingDirectory: String, source: GMQLSource): Unit = {
    checkFolderForDownloads(workingDirectory, source)
    downloadSubfolders(workingDirectory, source)
  }

  /**
    * lists all files inside the working directory
    * @param source gmql source
    * @param workingDirectory objective directory
    * @return list of FTPFiles.
    */
  def getUnfilteredFiles(source: GMQLSource,workingDirectory: String): List[FTPFile] = {
    var filesReturn = List[FTPFile]()
    var filesOk = false
    val threadDownload = new Thread {
      override def run(): Unit = {
        try {
          var timesTried = 0
          while (timesTried < 4 && !filesOk) {
            val ftpDownload = new FTP()
            val connected = ftpDownload.connectWithAuth(
              source.url,
              source.parameters.filter(_._1 == "username").head._2,
              source.parameters.filter(_._1 == "password").head._2).getOrElse(false)
            if (connected) {
              if (ftpDownload.cd(workingDirectory).getOrElse(false)) {
                filesReturn = ftpDownload.listFiles()
                filesOk = true
                if (timesTried == 3) {
                  logger.warn("Connection lost with the FTP server, skipping")
                }
                else if (timesTried == 2) {
                  logger.info("Seems internet connection is lost, resuming in 5 minutes.")
                  Thread.sleep(1000 * 60 * 5)
                }
              }
              else
                logger.error(s"couldn't access directory $workingDirectory")
              timesTried += 1
              ftpDownload.disconnect()
            }
            else
              logger.error(s"couldn't connect to ${source.url}")
          }
        }
        catch {
          case ex: InterruptedException => logger.error(s"Listing files took too long, aborted by timeout")
          case ex: Exception => logger.error("Could not connect to the FTP server: " + ex.getMessage)
        }
      }
    }
    threadDownload.start()
    try {
      threadDownload.join(10 * 60 * 1000)
    }
    catch {
      case ex: InterruptedException =>
        logger.error(s"Could'nt list files")
    }
    filesReturn
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
    //id of the source with the name
    val sourceId = FileDatabase.sourceId(source.name)

    //start the iteration for every dataset.
    for (dataset <- source.datasets) {
      if (dataset.downloadEnabled) {
        val datasetId = FileDatabase.datasetId(sourceId, dataset.name)
        if (workingDirectory.matches(dataset.parameters.filter(_._1 == "folder_regex").head._2)) {
          val outputPath = source.outputFolder + File.separator + dataset.outputFolder + File.separator + "Downloads"
          //check existence of the directory
          logger.info("Searching: " + workingDirectory)
          if (!new java.io.File(outputPath).exists) {
            try {
              new java.io.File(outputPath).mkdirs()
            }
            catch {
              case ex: Exception => logger.error(s"could not create the folder $outputPath")
            }
          }

          val unfilteredFiles: List[FTPFile] = getUnfilteredFiles(source, workingDirectory)

          val files: List[FTPFile] =
            if (dataset.parameters.exists(_._1 == "files_regex"))
              unfilteredFiles.filter(_.isFile).filter(_.getName.matches(
                dataset.parameters.filter(_._1 == "files_regex").head._2))
            else
              List[FTPFile]()
          var counter = 0
          val total = files.size
          var md5Downloaded = false
          if (dataset.parameters.exists(_._1 == "md5_checksum_tcga2bed")) {
            val aux2 = dataset.parameters.filter(_._1 == "md5_checksum_tcga2bed")
            val aux1 = aux2.head._2
            val file = unfilteredFiles.filter(_.getName == aux1).head
            val url = workingDirectory + File.separator + file.getName
            val fileId = FileDatabase.fileId(datasetId, url, STAGE.DOWNLOAD, file.getName)
            md5Downloaded = downloadFile(fileId, "", file, outputPath, source, workingDirectory, url, 1, 1)
          }
          var expInfoDownloaded = false
          var totalFiles = 0
          if (dataset.parameters.exists(_._1 == "exp_info_tcga2bed")) {
            val aux2 = dataset.parameters.filter(_._1 == "exp_info_tcga2bed")
            val aux1 = aux2.head._2
            val file = unfilteredFiles.filter(_.getName == aux1).head
            val url = workingDirectory + File.separator + file.getName
            val fileId = FileDatabase.fileId(datasetId, url, STAGE.DOWNLOAD, file.getName)
            expInfoDownloaded = downloadFile(fileId, "", file, outputPath, source, workingDirectory, url, 1, 1)

            val nameAndCopyNumber: (String, Int) = FileDatabase.getFileNameAndCopyNumber(fileId)
            val expInfoName =
              if (nameAndCopyNumber._2 == 1) nameAndCopyNumber._1
              else nameAndCopyNumber._1.replaceFirst("\\.", "_" + nameAndCopyNumber._2 + ".")
            val expInfoFile = Source.fromFile(outputPath + File.separator + expInfoName)
            val aliquotLine = expInfoFile.getLines().filter(_.contains("aliquot_count")).map(_.split('\t').drop(1).head)
            if (aliquotLine.nonEmpty)
              totalFiles = aliquotLine.next().toInt * 2
            //2 times because the .bed and .meta
          }
          var downloadedFiles = 0
          for (file <- files) {
            val url = workingDirectory + File.separator + file.getName
            val fileId = FileDatabase.fileId(datasetId, url, STAGE.DOWNLOAD, file.getName)
            val hash =
              if (dataset.parameters.exists(_._1 == "md5_checksum_tcga2bed") && md5Downloaded) {
                val md5Filename = dataset.parameters.filter(_._1 == "md5_checksum_tcga2bed").head._2
                val md5Url = workingDirectory + File.separator + md5Filename
                val md5FileId = FileDatabase.fileId(datasetId, md5Url, STAGE.DOWNLOAD, md5Filename)

                val nameAndCopyNumber: (String, Int) = FileDatabase.getFileNameAndCopyNumber(md5FileId)
                val md5Name =
                  if (nameAndCopyNumber._2 == 1) nameAndCopyNumber._1
                  else nameAndCopyNumber._1.replaceFirst("\\.", "_" + nameAndCopyNumber._2 + ".")

                val md5File = Source.fromFile(outputPath + File.separator + md5Name)
                val lines = md5File.getLines().filterNot(_ == "")
                //                  if (lines.exists(_.split('\t').head == file.getName)) {
                val filteredLines = lines.filter(_.split('\t').head == file.getName).map(line => {
                  val hashCleaned = line.split('\t')
                  val hashCleanedDropped = hashCleaned.drop(1)
                  val hashAlone = hashCleanedDropped.head
                  hashAlone
                })
                if (filteredLines.nonEmpty)
                  filteredLines.next()
                else ""
              }
              else
                ""
            if (downloadFile(fileId, hash, file, outputPath, source, workingDirectory, url, counter+1, total))
              downloadedFiles = downloadedFiles + 1
            counter = counter + 1
          }
          if (expInfoDownloaded) {
            FileDatabase.runDatasetDownloadAppend(datasetId, dataset, totalFiles, downloadedFiles)
            if (totalFiles == downloadedFiles) {
              //add successful message to database, have to sum up all the dataset's folders.
              logger.info(s"All $totalFiles files for folder $workingDirectory of dataset ${dataset.name} of source ${source.name} downloaded correctly.")
            }
            else {
              //add the warning message to the database
              logger.warn(s"Dataset ${dataset.name} of source ${source.name} downloaded $downloadedFiles/$totalFiles files.")
            }
          }
          else
            logger.info(s"File count for dataset ${dataset.name} of source ${source.name} is not activated (check configuration xml).")
        }

      }
    }
  }

  /**
    * From: http://stackoverflow.com/questions/41642595/scala-file-hashing
    * calculates the md5 hash for a file.
    * @param path file location
    * @return hash code
    */
  def computeHash(path: String): String = {
    val buffer = new Array[Byte](8192)
    val md5 = MessageDigest.getInstance("MD5")

    val dis = new DigestInputStream(new FileInputStream(new File(path)), md5)
    try { while (dis.read(buffer) != -1) { } } finally { dis.close() }

    md5.digest.map("%02x".format(_)).mkString
  }

  /**
    * Downloads a file from an FTP server according to the fileDatabase protocol
    * @param fileId id for the file in the database
    * @param hash if not null, the hash code given by the source
    * @param file FTPfile to download
    * @param outputPath path for the download destination
    * @param source GMQLSource which contains the url and settings for the FTP connection
    * @param workingDirectory actual folder for the FTP connection
    * @param url full url for the downloaded file
    * @param counter indicates which number of file is downloading.
    * @param total total number of files to be downloaded.
    * @return if the download is done correctly
    */
  def downloadFile(
                    fileId:Int,hash: String,file: FTPFile,outputPath: String,source: GMQLSource,
                    workingDirectory: String,url: String,counter: Int,total: Int
                  ): Boolean ={
    var fileDownloaded = false
    if (FileDatabase.checkIfUpdateFile(
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
                logger.info(s"Downloading [$counter/$total]: " + url)
                if (ftpDownload.cd(workingDirectory).getOrElse(false)) {
                  downloaded = ftpDownload.downloadFile(file.getName, outputUrl).getOrElse(false)
                  if(downloaded){
                    val hashToCompare = computeHash(outputUrl)
                    if (hashToCompare != hash && hash != "") {
                      downloaded = false
                      if(timesTried==3){
                        logger.warn(s"file ${file.getName} was downloaded 3 times and failed the hash check, check correctness of hash value.")
                      }
                      else
                        logger.info(s"file ${file.getName} download does not match with hash, trying again.")
                    }
                  }
                  else{
                    if(!ftpDownload.connected){
                      logger.info("Internet connection lost, resuming in 5 minutes")
                      Thread.sleep(1000*60*5)
                    }
                  }
                }
                else
                  logger.error(s"couldn't access directory $workingDirectory")
                ftpDownload.disconnect()
              }
              else
                logger.error(s"couldn't connect to ${source.url}")
              timesTried += 1
              if (ftpDownload.connected)
                ftpDownload.disconnect()
            }
            if (!downloaded) {
              logger.error(s"Downloading [$counter/$total]: " + url + " FAILED")
              FileDatabase.markAsFailed(fileId)
            }
            else {
              logger.info(s"Downloading [$counter/$total]: " + url + " DONE")
              //here I have to get the hash and update it for the meta and the data files.
              //so I wait to get the meta file and then I mark the data file to updated
              val hash = computeHash(outputUrl)
              //get the hash, I will put the same on both files.
              FileDatabase.markAsUpdated(fileId, new File(outputUrl).length.toString, hash)
            }
            fileDownloaded = downloaded
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
        case ex: InterruptedException =>
          logger.error(s"Download of $url was interrupted")
      }
    }
    else{
      logger.info(s"File ${file.getName} is already up to date.")
      fileDownloaded = true
    }
    fileDownloaded
  }

  /**
    * downloads a file from ftp server.
    * @param outputPath local url
    * @param source gmql source
    * @param workingDirectory remote folder for the file
    * @param filename remote filename
    * @param hash remote hash
    * @param counter indicates which number of file is downloading.
    * @param total total number of files to be downloaded.
    * @return if the file was correctly downloaded.
    */
  def downloadFile(outputPath: String, source: GMQLSource,
                   workingDirectory: String, filename: String, hash:String,
                   counter: Int, total: Int): Boolean = {
    var fileDownloaded = false
    val url = workingDirectory + File.separator + filename
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
              logger.info(s"Downloading [$counter/$total]: " + url)
              if (ftpDownload.cd(workingDirectory).getOrElse(false)) {
                downloaded = ftpDownload.downloadFile(filename, outputPath).getOrElse(false)
                if (downloaded) {
                  val hashToCompare = computeHash(outputPath)
                  if (hashToCompare != hash && hash != "") {
                    downloaded = false
                    if (timesTried == 3) {
                      logger.warn(s"file $filename was downloaded 3 times and failed the hash check, check correctness of hash value.")
                    }
                    else
                      logger.info(s"file $filename download does not match with hash, trying again.")
                  }
                }
                else {
                  if (!ftpDownload.connected) {
                    logger.info("Internet connection lost, resuming in 5 minutes")
                    Thread.sleep(1000 * 60 * 5)
                  }
                }
              }
              else
                logger.error(s"couldn't access directory $workingDirectory")
              ftpDownload.disconnect()
            }
            else
              logger.error(s"couldn't connect to ${source.url}")
            timesTried += 1
            if (ftpDownload.connected)
              ftpDownload.disconnect()
          }
          if (!downloaded) {
            logger.error(s"Downloading [$counter/$total]: " + url + " FAILED")
          }
          else {
            logger.info(s"Downloading [$counter/$total]: " + url + " DONE")
            //here I have to get the hash and update it for the meta and the data files.
            //so I wait to get the meta file and then I mark the data file to updated
            val hash = computeHash(outputPath)
            //get the hash, I will put the same on both files.
//            FileDatabase.markAsUpdated(fileId, new File(outputPath).length.toString, hash)
          }
          fileDownloaded = downloaded
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
      case ex: InterruptedException =>
        logger.error(s"Download of $url was interrupted")
    }
    fileDownloaded
  }




  /**
    * Finds all subfolders in the working directory and performs checkFolderForDownloads on it
    *
    * @param workingDirectory    current folder of the ftp connection
    * @param source configuration for downloader, folders for input and output by regex and also for files
    */
  private def downloadSubfolders(workingDirectory: String, source: GMQLSource): Unit = {

    val directories: Array[FTPFile] = getDirectories(source,workingDirectory)
    directories.foreach(directory =>
      recursiveDownload(
        if (workingDirectory.endsWith(File.separator)) workingDirectory + directory.getName
        else workingDirectory + File.separator + directory.getName,
        source))
  }

  /**
    * lists the directories in the current working directory
    * @param source gmql source
    * @param workingDirectory object directory
    * @return array of directories.
    */
  def getDirectories(source: GMQLSource, workingDirectory: String): Array[FTPFile] = {
    var filesReturn = Array[FTPFile]()
    var filesOk = false
    val threadDownload = new Thread {
      override def run(): Unit = {
        try {
          logger.info("working directory: " + workingDirectory)
          var timesTried = 0
          while (timesTried < 4 && !filesOk) {
            val ftpDownload = new FTP()
            val connected = ftpDownload.connectWithAuth(
              source.url,
              source.parameters.filter(_._1 == "username").head._2,
              source.parameters.filter(_._1 == "password").head._2).getOrElse(false)
            if (connected) {
              if (ftpDownload.cd(workingDirectory).getOrElse(false)) {
                filesReturn = ftpDownload.listDirectories()
                filesOk = true
                if (timesTried == 3) {
                  logger.warn("Connection lost with the FTP server, skipping")
                }
                else if (timesTried == 2) {
                  logger.info("Seems internet connection is lost, resuming in 5 minutes.")
                  Thread.sleep(1000 * 60 * 5)
                }
              }
              else
                logger.error(s"couldn't access directory $workingDirectory")
              timesTried += 1
              ftpDownload.disconnect()
            }
            else
              logger.error(s"couldn't connect to ${source.url}")
          }
        }
        catch {
          case ex: InterruptedException => logger.error(s"Listing files took too long, aborted by timeout")
          case ex: Exception => logger.error("Could not connect to the FTP server: " + ex.getMessage)
        }
      }
    }
    threadDownload.start()
    try {
      threadDownload.join(10 * 60 * 1000)
    }
    catch {
      case ex: InterruptedException =>
        logger.error(s"Could'nt list files")
    }
    filesReturn
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
  override def downloadFailedFiles(source: GMQLSource): Unit = {
    logger.info(s"Downloading failed files for source ${source.name}")
    val sourceId = FileDatabase.sourceId(source.name)
    source.datasets.foreach(dataset => {
      var downloadedFiles = 0
      var counter = 0
      logger.info(s"Downloading failed files for dataset ${dataset.name}")
      val datasetId = FileDatabase.datasetId(sourceId,dataset.name)
      val failedFiles = FileDatabase.getFailedFiles(datasetId,STAGE.DOWNLOAD)
      val totalFiles = failedFiles.size
      //in file I have (fileId,name,copyNumber,url, hash)
      failedFiles.foreach(file =>{
        val workingDirectory = file._4.substring(0,file._4.lastIndexOf(File.separator))
        val filename =
          if (file._3 == 1) file._2
          else file._2.replaceFirst("\\.", "_" + file._3 + ".")
        val filePath =
          source.outputFolder + File.separator + dataset.outputFolder +
            File.separator + "Downloads" + File.separator + filename
        counter = counter+1
        if(downloadFile(filePath,source,workingDirectory,file._2,file._5,counter,totalFiles)){
            downloadedFiles = downloadedFiles + 1
            val downloadedFile = new File(filePath)
            FileDatabase.markAsUpdated(file._1, downloadedFile.length.toString)
          }
        else
          FileDatabase.markAsFailed(file._1)
      })
      FileDatabase.runDatasetDownloadAppend(datasetId,dataset,0,downloadedFiles)
    })
  }
}
