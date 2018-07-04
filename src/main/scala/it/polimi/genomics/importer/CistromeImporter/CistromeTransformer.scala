package it.polimi.genomics.importer.CistromeImporter

import java.io._
import java.util

import it.polimi.genomics.importer.GMQLImporter.{GMQLDataset, GMQLSource, GMQLTransformer}
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

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
    if (new File(originPath + File.separator + originalFilename).exists()) {
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
    val path = source.outputFolder + File.separator + dataset.outputFolder + File.separator + "Downloads" + File.separator + filename
    val pathTransformations = source.outputFolder + File.separator + dataset.outputFolder + File.separator + "Transformations" + File.separator + "temp" + File.separator
    new File(pathTransformations).mkdirs()

    println("path:", path)
    val list = ListBuffer.empty[String]

    val fileNameList = dataset.parameters.filter(_._1 == "file_filter").map(_._2).toList ++ List(".tar.gz")

    untarListRecursively(path, pathTransformations, list, fileNameList)

    list.filterNot(_.contains(".bed")).foreach(println)

    list.toList
  }


  def checkFileName(listFilename: List[String], filename:String) = listFilename.map(filename.contains(_)).fold(false)(_||_)


  /**
    * extracts the files inside a tar container and returns a list of files
    *
    * @return Java ArrayList of File
    */
  private def untarListRecursively(path: String, pathTransformations: String, list: ListBuffer[String], listFilename: List[String]): Unit = {
    val fin = new FileInputStream(path)
    val in = new BufferedInputStream(fin)
    val gzIn = new GzipCompressorInputStream(in)
    val tarIn = new TarArchiveInputStream(gzIn)
    var entry = tarIn.getNextEntry

    while (entry != null) {
      println(entry.getName)
      if (!entry.isDirectory && checkFileName(listFilename, entry.getName) ) {
        val data = new Array[Byte](2048)
        val fos = new FileOutputStream(pathTransformations + entry.getName)
        val dest = new BufferedOutputStream(fos, 2048)
        var count = tarIn.read(data, 0, 2048)
        while (count != -1) {
          dest.write(data, 0, count)
          count = tarIn.read(data, 0, 2048)
        }
        dest.close()

        if (entry.getName.endsWith(".tar.gz")) {
          untarListRecursively(pathTransformations + entry.getName, pathTransformations, list, listFilename)
          new File(pathTransformations + entry.getName).delete()
        }
        else {
          list.append(entry.getName)
        }
      } else {
        val folder = new File(pathTransformations + entry.getName)
        folder.mkdirs()

      }

      entry = tarIn.getNextEntry
    }
    tarIn.close()
    gzIn.close()
    in.close()
    fin.close()
  }

}
