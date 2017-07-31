package it.polimi.genomics.importer.CistromeImporter

import it.polimi.genomics.importer.GMQLImporter.{GMQLDataset, GMQLSource, GMQLTransformer}
import java.io._

import scala.collection.mutable
import scala.io.Source
import java.util
import java.util.zip.GZIPInputStream

import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveInputStream}
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.log4j.{ConsoleAppender, Level, Logger, PatternLayout}
import org.slf4j.LoggerFactory

/**
  * Created by nachon on 7/28/17.
  */
class CistromeTransformer extends GMQLTransformer {
  val logger = LoggerFactory.getLogger(this.getClass)
  /**
    * recieves .json and .bed.gz files and transform them to get metadata in .meta files and region in .bed files.
    *
    * @param source           source where the files belong to.
    * @param originPath       path for the  "Downloads" folder
    * @param destinationPath  path for the "Transformations" folder
    * @param originalFilename name of the original file .json/.gz
    * @param filename         name of the new file .meta/.bed
    * @return List(fileId, filename) for the transformed files.
    */
  override def transform(source: GMQLSource, originPath: String, destinationPath: String, originalFilename: String, filename: String): Boolean = {
    if(new File(originPath+File.separator+originalFilename).exists()) {
      untarRecursively(originPath + File.separator + originalFilename, destinationPath+File.separator, filename)
      true
    }
    else
      false
  }
  /**
    * by receiving an original filename returns the new GDM candidate name(s).
    *
    * @param filename original filename
    * @param dataset  dataset where the file belongs to
    * @param source   source where the files belong to.
    * @return candidate names for the files derived from the original filename.
    */
  override def getCandidateNames(filename: String, dataset: GMQLDataset, source: GMQLSource): List[String] = {
    val path = source.outputFolder + File.separator + dataset.outputFolder + File.separator + "Downloads"+File.separator+filename

    val list = new util.ArrayList[String]()
    untarListRecursively(path,list)

    fromJavaToScala(list)
  }

  /**
    * looks inside a tar.gz container for a file, if another tar.gz is inside, it will search inside recursively
    * @param path tar.gz location
    * @param outputFolder output folder for the extracted file
    * @param filename name of the file to search
    */
  private def untarRecursively(path: String, outputFolder: String, filename: String): Unit ={
    val fin = new FileInputStream(path)
    val in = new BufferedInputStream(fin)
    val gzIn = new GzipCompressorInputStream(in)
    val tarIn = new TarArchiveInputStream(gzIn)
    var entry = tarIn.getNextEntry
    while (entry != null) {
      if(entry.getName == filename) {
        val data = new Array[Byte](2048)
        val fos = new FileOutputStream(outputFolder + entry.getName)
        val dest = new BufferedOutputStream(fos, 2048)
        var count = tarIn.read(data, 0, 2048)
        while (count != -1) {
          dest.write(data, 0, count)
          count = tarIn.read(data, 0, 2048)
        }
        dest.close()
      }
      else if(entry.getName.endsWith(".tar.gz")) {
        val data = new Array[Byte](2048)
        val fos = new FileOutputStream(outputFolder + entry.getName)
        val dest = new BufferedOutputStream(fos, 2048)
        var count = tarIn.read(data, 0, 2048)
        while (count != -1) {
          dest.write(data, 0, count)
          count = tarIn.read(data, 0, 2048)
        }
        dest.close()
        untarRecursively(outputFolder+entry.getName,outputFolder,filename)
        new File(outputFolder+entry.getName).delete()
      }
      entry = tarIn.getNextEntry
    }
  }
  private def fromJavaToScala(list: util.ArrayList[String]): List[String] = {
    var aux = ""
    for(i <- 0 until list.size()){
      aux += list.get(i)
      aux += ","
    }
    aux.substring(0,aux.length-1).split(",").toList
  }
  /**
    * extracts the files inside a tar container and returns a list of files
    * @return Java ArrayList of File
    */
  private def untarListRecursively(path:String, list: util.ArrayList[String]): Unit ={
    val fin = new FileInputStream(path)
    val in = new BufferedInputStream(fin)
    val gzIn = new GzipCompressorInputStream(in)
    val tarIn = new TarArchiveInputStream(gzIn)
    var entry = tarIn.getNextEntry
    while (entry != null) {
      if(entry.getName.endsWith(".tar.gz")) {
        val data = new Array[Byte](2048)
        val fos = new FileOutputStream(path + entry.getName)
        val dest = new BufferedOutputStream(fos, 2048)
        var count = tarIn.read(data, 0, 2048)
        while (count != -1) {
          dest.write(data, 0, count)
          count = tarIn.read(data, 0, 2048)
        }
        dest.close()
        untarListRecursively(path + entry.getName, list)
        new File(path + entry.getName).delete()
      }
      else{
        list.add(entry.getName)
      }
      entry = tarIn.getNextEntry
    }
  }
  /**
    * extracts the gzipFile into the outputPath.
    *
    * @param gzipFile   full location of the gzip
    * @param outputPath full path of destination, filename included.
    */
  def unGzipIt(gzipFile: String, outputPath: String): Boolean = {
    val bufferSize = 1024
    val buffer = new Array[Byte](bufferSize)
    var unGzipped = false
    var timesTried = 0
    while (timesTried < 4 && !unGzipped) {
      try {
        val zis = new GZIPInputStream(new BufferedInputStream(new FileInputStream(gzipFile)))
        val newFile = new File(outputPath)
        val fos = new FileOutputStream(newFile)

        var ze: Int = zis.read(buffer)
        while (ze >= 0) {

          fos.write(buffer, 0, ze)
          ze = zis.read(buffer)
        }
        fos.close()
        zis.close()
        unGzipped = true
      } catch {
        case e: IOException => timesTried += 1
      }
    }
    if (!unGzipped)
      logger.error("Couldnt UnGzip the file: " + outputPath)
    unGzipped
  }
}
