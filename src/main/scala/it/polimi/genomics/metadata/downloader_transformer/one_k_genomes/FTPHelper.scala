package it.polimi.genomics.metadata.downloader_transformer.one_k_genomes

import java.util.Calendar

import it.polimi.genomics.metadata.downloader_transformer.default.utils.Ftp
import it.polimi.genomics.metadata.step.xml
import it.polimi.genomics.metadata.util.PatternMatch
import org.apache.commons.net.ftp.FTPFile
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Try}

/**
 *   Created by Tom on Sep, 2019
 */
class FTPHelper {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val client = new Ftp()
  val localDownloadDir = s"${sys.props.get("user.dir").get}/../Metadata-Manager-WorkDir/MD/"

  // debug fields
  val serverAddress = "ftp.1000genomes.ebi.ac.uk" // FTP server address without final slash
  val optionalRemotePath = "vol1/ftp/"  //final slash is optional
  val username = "anonymous"
  val password = "anonymous"
  val currentTime: Calendar = Calendar.getInstance()
  val fileProperties: Map[String, Any] = Map("timestamp" -> currentTime, "size" -> 918)


  def test(source: xml.Source) : Unit = {

  }

  def downloadFile(url: String, username: String, password: String): Option[String] = {
    // split the ftp address in (base server address) + (optional path to a directory) + (filename)
    val parts = PatternMatch.matchParts(url, PatternMatch.createPattern("([^/]*)/(.*/)?(.*)"))
    var output = None : Option[String]
    if(parts.nonEmpty){
      try {
        val serverAddr = parts.head
        val optionalPath = parts(1)
        val filename = parts.last
        client.connectWithAuth(serverAddr, username, password)
        moveToLocation(optionalPath)
        val outputFilePath = s"$localDownloadDir$filename"
        client.downloadFile(filename, outputFilePath)
        client.disconnect()
        output = Some(outputFilePath)
      } catch {
//        TODO retry
        case ex: Exception =>
          ex.printStackTrace()
      }
    } else throw new IllegalArgumentException("INVALID URL LOCATION")
    output
  }

  def listContentLocation(url: String, username: String, password: String): Try[List[FTPFile]] = {
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

