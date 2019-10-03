package it.polimi.genomics.metadata.downloader_transformer.one_k_genomes

import java.io.IOException
import java.nio.file.{Files, Paths}

import it.polimi.genomics.metadata.downloader_transformer.default.utils.Ftp
import it.polimi.genomics.metadata.step.xml
import it.polimi.genomics.metadata.step.xml.Dataset
import it.polimi.genomics.metadata.util.{FileUtil, PatternMatch}
import org.apache.commons.net.ftp.{FTPConnectionClosedException, FTPFile}
import org.apache.commons.net.io.CopyStreamException
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

/**
 *   Created by Tom on Sep, 2019
 */
class FTPHelper(dataset: Dataset) {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val client = new Ftp()
  private val username = dataset.getParameter("FTP_username").get
  private val password = dataset.getParameter("FTP_password").get

  def test(source: xml.Source) : Unit = {

  }

  /**
   * Tries to download the file at the given URL argument into the directory defined as:
   * < xml config file -> settings -> base_working_directory>/< source name>/< dataset name>/Downloads/
   * @param url the URL of the file to download
   * @param numberOfAttempts number of attempts to download the file before giving up
   * @return the path to the file downloaded in the local file system if the operation succeeds, an exception otherwise
   */
  def downloadFile(url: String, numberOfAttempts: Int = 3): Try[String] = {
    // split the ftp address in (base server address) + (optional path to a directory) + (filename)
    val parts = PatternMatch.matchParts(url, PatternMatch.createPattern("([^/]*)/(.*/)?(.*)"))
    if (parts.isEmpty)
      Failure(new IllegalArgumentException("CAN'T IDENTIFY URL PARTS OF : " + url))
    val serverAddr = parts.head
    val optionalPath = parts(1)
    val filename = parts.last

    val downloadDir = DatasetInfo.getDownloadDir(dataset)
    FileUtil.createLocalDirectory(DatasetInfo.getDownloadDir(dataset))
    val outputFilePath = s"$downloadDir$filename"
    Files.deleteIfExists(Paths.get(outputFilePath))

    def tryDownload(attemptsLeft: Int, lastException: Exception, socketDataTimeoutMillis: Int): Try[Boolean] = {
      /* Wrap whole procedure in a Try to catch exception on connection/disconnection and operations other than the download */
      Try({
        /* if the download fails, retry attemptsLeft times before throwing an exception (the last one) */
        if (attemptsLeft > 0) {
          logger.debug("ATTEMPT: #"+(numberOfAttempts-attemptsLeft+1)+" OF "+numberOfAttempts)
          client.connectWithAuth(serverAddr, username, password)
          moveToLocation(optionalPath)
          val result = client.downloadFileOrResume(filename, outputFilePath)
          client.disconnect()
          /* the following are the only expected exceptions happening during download that you can hope to recover from */
          result match {
            case Failure(ex: FTPConnectionClosedException) =>
              logger.warn("DOWNLOAD BLOCKED. SERVER REPLIED WITH CODE 421. DETAILS: ", ex)
              if(attemptsLeft - 1 > 0) {
                logger.warn("NEW ATTEMPT IN 1 MINUTE")
                Thread.sleep(1000 * 60)
              }
              tryDownload(attemptsLeft - 1, ex, socketDataTimeoutMillis).get
            case Failure(ex: CopyStreamException) =>
              logger.warn("TRANSFER WAS INTERRUPTED. DETAILS: ", ex)
              if(attemptsLeft -1 > 0) {
                logger.warn("RETRYING DOWNLOAD IN 5 SECOND. SET DATA CONNECTION TIMEOUT + 1 sec")
                Thread.sleep(5000)
              }
              tryDownload(attemptsLeft - 1, ex, socketDataTimeoutMillis+1000).get
            case Failure(ex: IOException) =>
              logger.warn("EXCEPTION WHILE DOWNLOADING. CHECK CONNECTION TO INTERNET. DETAILS: ", ex)
              if(attemptsLeft -1 > 0) {
                logger.warn("NEW ATTEMPT IN 1 MINUTE")
                Thread.sleep(1000 * 60)
              }
              tryDownload(attemptsLeft - 1, ex, socketDataTimeoutMillis).get
            case Success(_) => result.get
          }
        } else
          throw lastException
      })
    }

    logger.info("BEGIN DOWNLOAD OF "+url)
    val downloadCompleted = tryDownload(numberOfAttempts, null, getDataConnectionTimeoutParam())

    downloadCompleted match {
      case Success(_) =>
        logger.info("DOWNLOAD COMPLETED")
        Success(outputFilePath)
      case Failure(cause) =>
        logger.debug("DOWNLOAD FAILED")
        Failure(cause)
    }
  }

  /**
   * Tries to download the file at the given URL argument into the directory defined as:
   * < xml config file -> settings -> base_working_directory>/< source name>/< dataset name>/Downloads/.
   * Calling this method is equal to calling FTPHelper.downloadFile(url, x) with x = FTPHelper.suggestDownloadAttemptsNum.
   * @param url the URL of the file to download
   * @param expectedFileSize this value is used to estimate a reasonable number of download attempts before giving up
   * @return the path to the file downloaded in the local file system if the operation succeeds, an exception otherwise
   */
  def downloadFile(url: String, expectedFileSize: Long): Try[String] = {
    downloadFile(url, FTPHelper.suggestDownloadAttemptsNum(expectedFileSize))
  }

  // DEVELOPMENT ONLY
  def testDownload(serverBaseAddr: String, optionalPath: String, filename: String, user: String, passw: String): Try[String] = {
    val downloadDir = DatasetInfo.getDownloadDir(dataset)
    FileUtil.createLocalDirectory(DatasetInfo.getDownloadDir(dataset))
    val outputFilePath = s"$downloadDir$filename"

    def tryDownload(attemptsLeft: Int, lastException: Exception): Try[Boolean] = {
      /* Wrap whole procedure in a Try to catch exception on connection/disconnection and operations other than the download */
      Try({
        /* if the download fails, retry attemptsLeft times before throwing an exception (the last one) */
        if (attemptsLeft > 0) {
          client.connectWithAuth(serverBaseAddr, user, passw)
          moveToLocation(optionalPath)
          val result = client.downloadFileOrResume(filename, outputFilePath)
          client.disconnect()
          /* the following are the only expected exceptions happening during download that you can hope to recover from */
          result match {
            case Failure(ex: FTPConnectionClosedException) =>
              println("RETRYING DOWNLOAD IN 1 MINUTE. SERVER REPLIED WITH CODE 421. DETAILS: ", ex)
              Thread.sleep(1000 * 60)
              tryDownload(attemptsLeft - 1, ex).get
            case Failure(ex: CopyStreamException) =>
              println("RETRYING DOWNLOAD IN 1 SECOND.TRANSFER WAS INTERRUPTED. DETAILS: ", ex)
              Thread.sleep(1000)
              tryDownload(attemptsLeft - 1, ex).get
            case Failure(ex: IOException) =>
              println("EXCEPTION WHILE DOWNLOADING. RETRYING. DETAILS: ", ex)
              tryDownload(attemptsLeft - 1, ex).get
            case Success(_) => result.get
          }
        } else
          throw lastException
      })
    }

    val downloadCompleted = tryDownload(3, null)

    downloadCompleted match {
      case Success(_) => Success(outputFilePath)
      case Failure(cause) => Failure(cause)
    }
  }

  def listContentLocation(url: String): Try[List[FTPFile]] = {
    // split the ftp address in (base server address) + (optional path to a directory)
    val parts = PatternMatch.matchParts(url, PatternMatch.createPattern("([^/]*)/(.*)"))
    if(parts.isEmpty)
      Failure(new Throwable("INVALID URL LOCATION"))
    else
      Try({
        val serverAddr = parts.head
        val optionalPath = parts.last
        client.connectWithAuth(serverAddr, username, password)
        moveToLocation(optionalPath)
        val content = client.listFiles()
        client.disconnect()
        content
      })
  }

  // DEVELOPMENT ONLY
  def testListContentLocation(serverBaseAddr: String, optionalPath: String, user: String, passw: String): Try[List[FTPFile]] = {
    Try({
      client.connectWithAuth(serverBaseAddr, user, passw)
      moveToLocation(optionalPath)
      val content = client.listFiles()
      client.disconnect()
      content
    })
  }

  /**
   * Explore the server depth-first starting from the given directory URL and returns list of directory URLs
   * associated with the files contained in each.
   *
   * @param dirURL the directory where to start traversing the server. Parent and sibling directories won't be explored.
   * @param fileNameRegex optional regular expression in Java form applied to the name of the files. If present, only
   *                      the matching filenames will be included in the resulting List.
   * @param excludeSubdirs flag telling whether to limit the exploration to the given argument directory URL
   * @return a List of tuples, one for each directory traversed containing valid filenames. Each tuple contains the
   *         URL of a directory or subdirectory, and a non-empty List of FTPFile(s). If excludeSubdirs is true, the
   *         List produced by this method can only have size 1 or 0 (it's 0 if there aren't files with names matching
   *         the regex or if the directory is empty).
   */
  def exploreServer(dirURL: String, fileNameRegex: Option[String], excludeSubdirs: Boolean): List[(String, List[FTPFile])] = {
    def explore(currentDir: String): List[(String, List[FTPFile])] = {
      logger.debug("SEARCHING MATCHING FILES IN DIRECTORY "+currentDir)
      val dirContent = listContentLocation(currentDir).get
      val files = filterFilesOnly(dirContent)
      val filteredFiles = if(fileNameRegex.isDefined) filterByName(files , fileNameRegex.get) else files
      /* Creates a tuple (directory URL, List[FTPFiles]) and wraps it inside a List if filteredFiles isn't empty.
      * Wrapping the tuple in a List is necessary later on to concatenate tuples inside a unique List */
      val dirURL_File_List = if(filteredFiles.nonEmpty) List((currentDir, filteredFiles)) else List.empty[(String, List[FTPFile])]
      val subDirs = filterDirectoriesOnly(dirContent)
      // concatenate the result of the recursive call on subdirectories
      dirURL_File_List ++ (
        if(excludeSubdirs || subDirs.isEmpty)
          List.empty[(String, List[FTPFile])]
        else {
          subDirs.flatMap(subDirFile => {
            val subDirURL = s"$currentDir${subDirFile.getName}/"
            explore(subDirURL)
          })
        })
    }

    explore(dirURL)
  }

  def readProperties(file: FTPFile) : Map[String, Any] = {
    val props = Map("timestamp" -> file.getTimestamp,
      "size" -> file.getSize)
/*      //  debug
    println("FILE PROPERTIES:")
    props.foreach { case (str, value) => println(s"$str: ${value.toString}")}*/
    props
  }

  /**
   * Can move into nested folder hierarchies at any depth level.
   * Moving to parent directory is achieved with the argument path "../"
   * If the given argument doesn't correspond to a valid hop, the current directory is unchanged
   *
   * @param pathFromFTPRoot a string containing only folder names and / describing the sequence of hops from the
   *                        current directory to any folder. Leading and trailing / can be omitted.
   */
  private def moveToLocation(pathFromFTPRoot: String) : Unit = {
    if(client.connected && pathFromFTPRoot.nonEmpty){
      logger.info(s"UPDATING WORKING DIRECTORY FROM ${client.workingDirectory()} TO $pathFromFTPRoot")
      client.cd(pathFromFTPRoot).getOrElse(println("UPDATE FAILED"))
      logger.info("CURRENT WORKING DIRECTORY " + client.workingDirectory())
    }
  }

  def filterFilesOnly(files: List[FTPFile]): List[FTPFile] = {
    files.filter(_.isFile)
  }

  def filterDirectoriesOnly(files: List[FTPFile]): List[FTPFile] = {
    files.filter(_.isDirectory)
  }

  /**
   * Note that Java regex works differently from others: a regex test with "bcd" will return true only only in case
   * of a perfect match; so a test on "abcdefg" will return false. Use (.*) as prefix and postfix to the regex
   * to make the previous example return true.
   *
   * @param files List of FTPFiles to be filtered
   * @param regex the regular expression used as filter for the names of files and directories.
   * @return the filtered list of FTPFiles according to the regex given as argument. If regex is None, the same list
   *         of files in input is returned without changes.
   */
  def filterByName(files: List[FTPFile], regex: String): List[FTPFile] = {
//    println(s"regex: $regex")
    val filteredFiles = files.filter(_.getName.matches(regex))
    /*      // uncomment for debugging
        System.out.println("UNFILTERED FILE NAMES")
        files.foreach(f => {
          System.out.println(f.getName)
        })
        System.out.println("FILTERED FILE NAMES")
        filteredFiles.foreach(f => {
          System.out.println(f.getName)
        })*/
    filteredFiles
  }

  // DEBUG METHODS

  private def printDirectories(): Unit = {
    println("DIRECTORIES IN CURRENT DIR: ")
    val fileNames = client.listDirectories()
    for(name <- fileNames){
      println(name.getName)
    }
  }

  private def printFilesAndDirectories(): Unit = {
    println("ALL IN CURRENT DIR: ")
    val fileNames = client.listFiles()
    for(name <- fileNames){
      println(name.getName)
    }
  }

  def getDataConnectionTimeoutParam(): Int = {
    dataset.getParameter("data_connection_timeout").getOrElse("1000").toInt
  }
}

object FTPHelper {
  def suggestDownloadAttemptsNum(fileSizeInBytes: Long): Int ={
    val sizeInMB = fileSizeInBytes / 1048576
    (sizeInMB.floatValue()/200).ceil.toInt.max(3)
  }
}

