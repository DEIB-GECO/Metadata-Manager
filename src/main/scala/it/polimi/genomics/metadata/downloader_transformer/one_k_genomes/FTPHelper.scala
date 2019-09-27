package it.polimi.genomics.metadata.downloader_transformer.one_k_genomes

import java.io.File
import java.util.Calendar

import it.polimi.genomics.metadata.downloader_transformer.default.utils.Ftp
import it.polimi.genomics.metadata.step.xml
import it.polimi.genomics.metadata.step.xml.Dataset
import it.polimi.genomics.metadata.util.{FileUtil, PatternMatch}
import org.apache.commons.net.ftp.FTPFile
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
   * @return the path to the file downloaded in the local file system if the operation succeeds, an exception otherwise
   */
  def downloadFile(url: String): Try[String] = {
    // split the ftp address in (base server address) + (optional path to a directory) + (filename)
    val parts = PatternMatch.matchParts(url, PatternMatch.createPattern("([^/]*)/(.*/)?(.*)"))
    if(parts.isEmpty)
      Failure(new IllegalArgumentException("INVALID ARGUMENT URL"))
    try {
      val serverAddr = parts.head
      val optionalPath = parts(1)
      val filename = parts.last
      client.connectWithAuth(serverAddr, username, password)
      moveToLocation(optionalPath)
      val downloadDir = DatasetFilter.getDownloadDir(dataset)
      FileUtil.createLocalDirectory(downloadDir)
      val outputFilePath = s"$downloadDir$filename"
      client.downloadFile(filename, outputFilePath)
      client.disconnect()
      Success(outputFilePath)
    } catch {
//        TODO retry
      case ex: Exception =>
        ex.printStackTrace()
        Failure(ex)
    }
  }

  // DEVELOPMENT ONLY
  def testDownload(serverBaseAddr: String, optionalPath: String, filename: String, user: String, passw: String): Try[String] = {
    try {
      client.connectWithAuth(serverBaseAddr, user, passw)
      moveToLocation(optionalPath)
      val downloadDir = DatasetFilter.getDownloadDir(dataset)
      FileUtil.createLocalDirectory(downloadDir)
      val outputFilePath = s"$downloadDir$filename"
      client.downloadFile(filename, outputFilePath)
      client.disconnect()
      Success(outputFilePath)
    } catch {
      //        TODO retry
      case ex: Exception =>
        ex.printStackTrace()
        Failure(ex)
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
      println(s"UPDATING WORKING DIRECTORY FROM ${client.workingDirectory()} TO $pathFromFTPRoot")
      client.cd(pathFromFTPRoot).getOrElse(println("UPDATE FAILED"))
      println("CURRENT WORKING DIRECTORY " + client.workingDirectory())
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
  def filterByName(files: List[FTPFile], regex: Option[String]): List[FTPFile] = {
    println(s"regex: $regex")
    val filteredFiles = if(regex.isDefined) files.filter(_.getName.matches(regex.get)) else files
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
}

