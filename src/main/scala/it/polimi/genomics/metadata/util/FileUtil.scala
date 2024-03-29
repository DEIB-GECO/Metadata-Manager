package it.polimi.genomics.metadata.util

import java.io.{BufferedReader, BufferedWriter, IOException, LineNumberReader}
import java.nio.file._

import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

/**
 * Created by Tom on set, 2019
 */
object FileUtil {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   *
   * @param fullFilePath a string describing the absolute location of a file in the local memory
   * @throws java.nio.file.InvalidPathException if the file path is not valid
   * @throws java.io.IOException if an error occurs while reading the file
   * @throws SecurityException if no rights to access the file
   */
  def md5Hash(fullFilePath: String): Try[String] ={
    Try({
      val stream = Files.newInputStream(Paths.get(fullFilePath))
      val hash = org.apache.commons.codec.digest.DigestUtils.md5Hex(stream)
      stream.close()
//      DEBUG
      println(s"HASH OF FILE $fullFilePath: $hash")
      hash
    })
  }

  /**
   *
   * @param fullFilePath a string describing the absolute location of a file in the local memory
   * @return an instance of BufferedReader to read the file
   * @throws java.nio.file.InvalidPathException if the file path is not valid
   * @throws java.io.IOException if an error occurs while opening the file
   * @throws SecurityException if no rights to access the file
   */
  def open(fullFilePath: String): Try[BufferedReader] = {
    Try(Files.newBufferedReader(Paths.get(fullFilePath)))
  }

  /**
   * This method creates a folder at the given path and any necessary parent directory.
   *
   * @param dirPath the directory path of the folder to create.
   *                No matter if it ends with a slash or not, neither the slash or backward-slash convention used
   * @throws SecurityException    if a security don't allow the verification of the existence or the creation of the
   *                              directory path given as argument
   * @throws InvalidPathException if the argument is malformed
   * @throws java.io.IOException  if an I/O error occurs
   * @throws FileAlreadyExistsException if the specified path points to an already existing file
   */
  def createLocalDirectory(dirPath: String): Unit = {
    Files.createDirectories(Paths.get(dirPath))
    // debug
//    println(Paths.get(dirPath).toAbsolutePath.toString)
  }

  /**
   * Copies a file to the specified destination replacing an already existing file with the same location. The file in the
   * target location will have the same attributes of the source file. If the source file is a symbolic link to a file,
   * the symbolic link is copied instead of the file itself.
   * @throws DirectoryNotEmptyException: if the target file cannot be replaced because it is a non-empty directory
   * @throws IOException: if an I/O error occurs when reading or writing
   * @throws SecurityException: if the user doesn’t have read/write permission
   * @throws InvalidPathException if the arguments are malformed paths
   */
  def copyFile(sourceFilePath: String, targetFilePath: String): Unit ={
    val source = Paths.get(sourceFilePath)
    val target = Paths.get(targetFilePath)
    // create target containing directory to prevent java.nio.file.NoSuchFileException
    createLocalDirectory(getContainingDirFromFilePath(targetFilePath))
    Files.copy(source, target, StandardCopyOption.COPY_ATTRIBUTES, StandardCopyOption.REPLACE_EXISTING, LinkOption.NOFOLLOW_LINKS)
  }

  /**
   * Moves a file to the specified destination replacing an already existing file with the same location. The file in the
   * target location will have the same attributes of the source file. If the source file is a symbolic link to a file,
   * the symbolic link is moved instead of the file itself.
   * @throws DirectoryNotEmptyException: if the target file cannot be replaced because it is a non-empty directory
   * @throws IOException: if an I/O error occurs when reading or writing
   * @throws SecurityException: if the user doesn’t have read/write permission
   * @throws InvalidPathException if the arguments are malformed paths
   */
  def moveFile(sourceFilePath: String, targetFilePath: String): Unit ={
    val source = Paths.get(sourceFilePath)
    val target = Paths.get(targetFilePath)
    // create target containing directory to prevent java.nio.file.NoSuchFileException
    createLocalDirectory(getContainingDirFromFilePath(targetFilePath))
    Files.move(source, target, StandardCopyOption.COPY_ATTRIBUTES, StandardCopyOption.REPLACE_EXISTING, LinkOption.NOFOLLOW_LINKS)
  }

  /**
   * Deletes the specified file if it exists.
   * @param filePath the full file path to delete
   */
  def deleteFile(filePath:String): Unit = {
    Files.deleteIfExists(Paths.get(filePath))
  }

  def getFileNameFromPath(filePath: String): String = {
    val separatorIndex = Math.max(filePath.lastIndexOf("/"), filePath.lastIndexOf("\\"))
    filePath.substring(separatorIndex+1)
  }

  def getContainingDirFromFilePath(filePath: String): String = {
    val separatorIndex = Math.max(filePath.lastIndexOf("/"), filePath.lastIndexOf("\\"))
    filePath.substring(0, separatorIndex+1)
  }

  /**
   * @param filePath path to a file
   * @return the size of the file in bytes
   * @throws InvalidPathException if the arguments are malformed paths
   * @throws IOException if an I/O error occurs when reading or writing
   * @throws SecurityException: if the user doesn’t have read permission
   */
  def size(filePath: String): Long = {
    Files.size(Paths.get(filePath))
  }

  def printFirstLines(reader: BufferedReader): Unit ={
    try {
      if(reader.markSupported()) {
        reader.mark(512)
        val someLines = List(reader.readLine(), reader.readLine(), reader.readLine())
        println("CONTENT OF FILE:")
        someLines.foreach(line => println(line))
        println("...")
        reader.reset()
      } else
        println("CAN'T READ FIRST LINES WITHOUT SIDE-EFFECTS")
    } catch {
      case ex: java.io.IOException =>
        println("ERROR. FILE NOT ACCESSIBLE OR MARK INVALIDATED")
        ex.printStackTrace()
    }
  }

  /**
   * @param targetFilePath a string describing the absolute location of a file in the local memory. If the file doesn't
   *                       exist it's created, otherwise the existing file is erased and overwritten.
   * @param content a List of strings. Each string is treated as a line of the file being written.
   * @throws java.nio.file.InvalidPathException if the file path is not valid
   * @throws java.io.IOException if an error occurs while writing the file
   * @throws SecurityException if no rights to write the file
   */
  def writeReplace(targetFilePath: String, content: List[String]): Unit ={
    // create & replace
    val writer = Files.newBufferedWriter(Paths.get(targetFilePath), StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)
    // write all elements without last newLine
    for(i <- 0 until content.length-1) {
      writer.write(content(i))
      writer.newLine()
    }
    writer.write(content.last)
    writer.close()
  }

  /**
   * @param targetFilePath a string describing the absolute location of a file in the local memory. If the file doesn't
   *                       exist it's created, otherwise the existing file is erased and overwritten.
   * @return a BufferedWriter instance that can be used to write at the target file path.
   * @throws java.nio.file.InvalidPathException if the file path is not valid
   * @throws java.io.IOException if an error occurs while writing the file
   * @throws SecurityException if no rights to write the file
   */
  def writeReplace(targetFilePath: String): Try[BufferedWriter] = {
    // create & replace
    Try(Files.newBufferedWriter(Paths.get(targetFilePath), StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE))
  }

  /**
   * @param targetFilePath a string describing the absolute location of a file in the local memory. If the file doesn't
   *                       exist it's created, otherwise the returned BufferedReader instance starts at the end of the file.
   * @param startOnNewLine decides if the returned BufferedReader instance should be prepared so as the first write operation
   *                       writes on a new line or on the same line of the last byte of an existing file.
   * @return a BufferedWriter instance that can be used to write at the target file path.
   * @throws java.nio.file.InvalidPathException if the file path is not valid
   * @throws java.io.IOException if an error occurs while writing the file
   * @throws SecurityException if no rights to write the file
   */
  def writeAppend(targetFilePath: String, startOnNewLine: Boolean = false): Try[BufferedWriter] = {
    Try({
      val path = Paths.get(targetFilePath)
      val writer = Files.newBufferedWriter(path, StandardOpenOption.CREATE,
        StandardOpenOption.WRITE, StandardOpenOption.APPEND)
      if (startOnNewLine && Files.size(path) != 0)
        writer.newLine()
      writer
    })
  }

  /**
   * @param targetFilePath a string describing the absolute location of a file in the local memory. If the file doesn't
   *                       exist it's created, otherwise the returned BufferedReader instance starts at the end of the file.
   * @throws java.nio.file.InvalidPathException if the file path is not valid
   * @throws java.nio.file.DirectoryNotEmptyException if the targetFilePath exists and it is a directory
   * @throws java.io.IOException if an error occurs while writing the file
   * @throws SecurityException if no rights to write the file
   */
  def createReplaceEmptyFile(targetFilePath: String): Unit ={
    val path = Paths.get(targetFilePath)
    Files.deleteIfExists(path)
    Files.createFile(path)
  }

  /**
   * High order function that performs the specified action for each line of the file being read.
   * The BufferedReader instance is automatically closed once the end of the file is reached.
   *
   * @param reader a BufferedReader instance of the file to scan line per line
   * @param doForEachLine the function to execute for each line of the file, starting from the specified reader position.
   *                      The function takes as input parameter the current line.
   * @throws java.io.IOException if an error occurs while reading the file
   */
  def scanFileAndClose(reader: BufferedReader, doForEachLine: String => Unit, progressNotifier: Option[ApproximateReadProgress] = None): Unit = {
    var line = reader.readLine()
    val notifyProgress = progressNotifier.isDefined
    try {
      while (line != null) {
        doForEachLine(line)
        line = reader.readLine()
        if (notifyProgress)
          progressNotifier.get.advanceOneStep()
      }
    } finally {
      reader.close()
    }
  }



  /**
   * Count the total number of lines in a file. Warning: this can take a while.
   * Test results show that reading a 1.02GB with 106983 lines took about 4.5 seconds on an old machine.
   *
   * @param fullFilePath path to a file
   * @return the number of lines in the file
   * @throws InvalidPathException if the arguments are malformed paths
   * @throws IOException if an I/O error occurs when reading or writing
   * @throws SecurityException: if the user doesn’t have read permission
   */
  def countLines(fullFilePath: String): Try[Long] ={
    Try({
      val reader = Files.newBufferedReader(Paths.get(fullFilePath))
      val counter = new LineNumberReader(reader)
      while(counter.skip(Long.MaxValue) > 0) { }  // while the file has > Long.MaxValue lines, add quickly Long.MaxValue
      while(counter.skip(Int.MaxValue) > 0) { }   // while the file has > Int.MaxValue lines, add quickly Int.MaxValue
                                                  // doing the same with Short doesn't improve performances
      val lineCount = counter.getLineNumber + 1 // +1 because counter starts counting from 0
      counter.close()
      lineCount
    })
  }
}

/**
 * This class wraps a set of parameters required to estimate the current advancement of a large file reading process, and
 * provides a method (advanceOneStep) which must be called every time a new line is read.
 * The more totalLines is accurate and the progress steps are evenly distributed (i.e. lines of equal length), the more
 * the progress estimate is accurate.
 * Every time the total advancement increases of progressNotificationStep %, onProgress is called to notify the owner.
 * @param totalLines total lines of the document to read
 * @param progressNotificationStep the interval, in percentage points, at which the owner must be notified
 * @param onProgress a function receiving the current progress status, which gets automatically called at every
 *                   progress increment in percentage point equal or greater than progressNotificationStep.
 */
class ApproximateReadProgress(totalLines: Long, progressNotificationStep: Double, onProgress: Double => Unit) {
  var linesRead: Double = 0
  var lastProgressNotified: Double = 0.0

  def advanceOneStep(): Unit = {
    linesRead +=1
    val currentProgress = linesRead/totalLines*100
    //      printf("LAST: %f\t ADV: %f %s \n", lastProgressNotified, currentProgress, "%")
    if(currentProgress >= lastProgressNotified+progressNotificationStep){
      lastProgressNotified = currentProgress
      onProgress(currentProgress)
    }
  }

}
object ApproximateReadProgress {

  /**
   * Simple method printing the received advancement status value on the console as an integer percentage value.
   * The advancement status is printed in between the optional parameter strings author and filename.
   */
  def simpleProgressNotification(author: String = "", filename: String = "", logger: Option[Logger] = None): Double => Unit = {
    if(logger.isDefined){
      progress => {
        logger.get.info("%s: READ ⁓%.0f %% OF FILE %s".format(author, progress, filename))
      }
    } else
      progress => printf("%s: READ ⁓%.0f %% OF FILE %s\n", author, progress, filename)
  }

}
