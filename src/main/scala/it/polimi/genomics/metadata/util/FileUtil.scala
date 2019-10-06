package it.polimi.genomics.metadata.util

import java.io.{BufferedReader, BufferedWriter, IOException}
import java.nio.file.{DirectoryNotEmptyException, FileAlreadyExistsException, Files, InvalidPathException, LinkOption, Paths, StandardCopyOption, StandardOpenOption}

import scala.util.Try

/**
 * Created by Tom on set, 2019
 */
object FileUtil {

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
      case ex: java.io.IOException => {
        println("ERROR. FILE NOT ACCESSIBLE OR MARK INVALIDATED")
        ex.printStackTrace()
      }
    }
  }

  def writeReplace(where: String, content: List[String]): Unit ={
    // create & replace
    val writer = Files.newBufferedWriter(Paths.get(where), StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)
    content.foreach(line => {
      writer.write(line)
      writer.newLine()
    })
    writer.close()
  }

  def writeReplace(where: String): BufferedWriter = {
    // create & replace
    Files.newBufferedWriter(Paths.get(where), StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)
  }
}
