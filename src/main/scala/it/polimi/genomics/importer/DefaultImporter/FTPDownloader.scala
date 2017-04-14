package it.polimi.genomics.importer.DefaultImporter

import java.io.{File, FileInputStream}
import java.security.{DigestInputStream, MessageDigest}

import it.polimi.genomics.importer.DefaultImporter.utils.FTP
import it.polimi.genomics.importer.FileDatabase.{FileDatabase, STAGE}
import it.polimi.genomics.importer.GMQLImporter.{GMQLDownloader, GMQLSource}
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
          val ftpFiles = new FTP
          if (ftpFiles.connectWithAuth(
            source.url,
            source.parameters.filter(_._1 == "username").head._2,
            source.parameters.filter(_._1 == "password").head._2).getOrElse(false)) {
            ftpFiles.cd(workingDirectory)

            val unfilteredFiles = ftpFiles.listFiles()
            val files: List[FTPFile] = unfilteredFiles.filter(_.isFile).filter(_.getName.matches(
              dataset.parameters.filter(_._1 == "files_regex").head._2))
            ftpFiles.disconnect()
            var md5Downloaded = false
            if (dataset.parameters.exists(_._1 == "md5_checksum_tcga2bed")) {
              val aux2 = dataset.parameters.filter(_._1 == "md5_checksum_tcga2bed")
              val aux1 = aux2.head._2
              val file = unfilteredFiles.filter(_.getName == aux1).head
              val url = workingDirectory + File.separator + file.getName
              val fileId = FileDatabase.fileId(datasetId, url, STAGE.DOWNLOAD, file.getName)
              md5Downloaded = downloadFile(fileId,"",file,outputPath,source,workingDirectory,url)
            }
            var expInfoDownloaded = false
            var totalFiles = 0
            if (dataset.parameters.exists(_._1 == "exp_info_tcga2bed")) {
              val aux2 = dataset.parameters.filter(_._1 == "exp_info_tcga2bed")
              val aux1 = aux2.head._2
              val file = unfilteredFiles.filter(_.getName == aux1).head
              val url = workingDirectory + File.separator + file.getName
              val fileId = FileDatabase.fileId(datasetId, url, STAGE.DOWNLOAD, file.getName)
              expInfoDownloaded = downloadFile(fileId,"",file,outputPath,source,workingDirectory,url)

              val nameAndCopyNumber: (String, Int) = FileDatabase.getFileNameAndCopyNumber(fileId)
              val expInfoName =
                if (nameAndCopyNumber._2 == 1) nameAndCopyNumber._1
                else nameAndCopyNumber._1.replaceFirst("\\.", "_" + nameAndCopyNumber._2 + ".")
              val expInfoFile = Source.fromFile(outputPath + File.separator + expInfoName)
              val aliquotLine = expInfoFile.getLines().filter(_.contains("aliquot_count")).map(_.split('\t').drop(1).head)
              if(aliquotLine.nonEmpty)
                totalFiles = aliquotLine.next().toInt*2
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
                  if(filteredLines.nonEmpty)
                    filteredLines.next()
                  else ""
                }
                else
                  ""
              if(downloadFile(fileId,hash,file,outputPath,source,workingDirectory,url))
                downloadedFiles = downloadedFiles + 1
            }
            if(expInfoDownloaded) {
              FileDatabase.runDatasetDownloadAppend(datasetId,totalFiles,downloadedFiles)
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
          else
            logger.error("connection lost with FTP, skipping " + workingDirectory)
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
    * @return if the download is done correctly
    */
  def downloadFile(
                    fileId:Int,
                    hash: String,
                    file: FTPFile,
                    outputPath: String,
                    source: GMQLSource,
                    workingDirectory: String,
                    url: String
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
                logger.info("Downloading: " + url)
                if (ftpDownload.cd(workingDirectory).getOrElse(false)) {
                  downloaded = ftpDownload.downloadFile(file.getName, outputUrl).getOrElse(false)

                  val hashToCompare = computeHash(outputUrl)
                  if (hashToCompare != hash && hash != "") {
                    if(timesTried<3){
                      downloaded = false
                      logger.warn(s"file ${file.getName} was downloaded 3 times and failed the hash check, check correctness of hash value.")
                    }
                    else
                      logger.info(s"file ${file.getName} download does not match with hash, trying again.")
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
