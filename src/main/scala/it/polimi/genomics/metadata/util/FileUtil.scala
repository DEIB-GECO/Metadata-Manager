package it.polimi.genomics.metadata.util

import java.io.BufferedReader
import java.nio.file.{Files, Paths}

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

  def createLocalDirectory(path: String): Try[Boolean] = {
    if (!new java.io.File(path).exists){
      Try(new java.io.File(path).mkdirs())
    } else
      Try(true)
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
}
