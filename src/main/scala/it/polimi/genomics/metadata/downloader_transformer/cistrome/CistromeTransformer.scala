package it.polimi.genomics.importer.CistromeImporter

import java.io._

import it.polimi.genomics.metadata.downloader_transformer.Transformer
import it.polimi.genomics.metadata.step.xml.{Dataset, Source}
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveInputStream}
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.io.IOUtils
import org.slf4j.LoggerFactory

import scala.io.BufferedSource

/**
  * Created by nachon on 7/28/17.
  */
class CistromeTransformer extends Transformer {
  val logger = LoggerFactory.getLogger(this.getClass)

  var metadataFileLines: Map[String, Array[String]] = _

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
  override def transform(source: Source, originPath: String, destinationPath: String, originalFilename: String, filename: String): Boolean = {
    val fileTransformationPath = destinationPath + File.separator + filename
    val fileDownloadPath = originPath + File.separator + originalFilename


    if (new File(originPath + File.separator + originalFilename).exists()) {
      if (filename.endsWith(".meta")) {
        val line = metadataFileLines(filename)

        //correct this function and set as true
        false
      }
      else {
        val inputStream = untarListRecursively(fileDownloadPath, Some(filename)).right.get
        IOUtils.copy(inputStream, new FileOutputStream(fileTransformationPath))
        true
      }



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
  override def getCandidateNames(filename: String, dataset: Dataset, source: Source): List[String] = {
    val path = source.outputFolder + File.separator + dataset.outputFolder + File.separator + "Downloads" + File.separator + filename
    //    println("path:", path)

    val fileRegex = dataset.parameters.find(_._1 == "file_filter").map(_._2).getOrElse("ERROR").r
    val metadataFileRegex = dataset.parameters.find(_._1 == "file_metadata").map(_._2).getOrElse("ERROR").r

    val list = untarListRecursively(path).left.get


    val relatedFiles = list.flatMap {
      case fileRegex(fileName) => Some(fileName)
      case metadataFileRegex(metadataFileName) =>
        val metadataStream = untarListRecursively(path, Some(metadataFileName)).right.get
        val bs = new BufferedSource(metadataStream)
        metadataFileLines = bs.getLines.map { line =>
          val splitted = line.split('\t')
          (splitted.last + ".meta", splitted)
        }.toMap
        metadataFileLines.keys
      case _ => None
    }


    relatedFiles
      //      //TODO remove map
      //      .map { x =>
      //      println("output", x)
      //      x
      //      }
      .toList

  }


  def checkFileName(listFilename: List[String], filename: String): Boolean = listFilename.map(filename.contains(_)).fold(false)(_ || _)


  private def untarListRecursively(path: String, unzipFileName: Option[String] = None): Either[Iterator[String], InputStream] = {
    untarListRecursively(new FileInputStream(path), unzipFileName)
  }


  /**
    * extracts the files inside a tar container and returns a list of files
    *
    * @return Java ArrayList of File
    */
  private def untarListRecursively(inputStream: InputStream, unzipFileName: Option[String]): Either[Iterator[String], InputStream] = {
    val tarArchiveInputStream = new TarArchiveInputStream(new GzipCompressorInputStream(inputStream))

    val entries = getTarIterator(tarArchiveInputStream)


    val resultList = entries.flatMap { entry =>
      val fileName = entry.getName.split("/").last
//      println(fileName)
      if (unzipFileName.contains(fileName))
        return Right(tarArchiveInputStream)

      val innerResults: Iterator[String] =
        if (entry.getName.endsWith(".tar.gz"))
          untarListRecursively(tarArchiveInputStream, unzipFileName) match {
            case Left(iterator) => iterator
            case x => return x
          }
        else
          Iterator.empty

      getPrependIterator(fileName, innerResults)
    }

    if (unzipFileName.isDefined) {
      resultList.size //DO nothing
      //cannot find yet
      Left(Iterator.empty)
    }
    else
      Left(resultList)
  }

  //it is a correct iterator, but it works with flatmap. I need to do as this, because I should not move the tarStream to the next entry
  private def getTarIterator(tarArchiveInputStream: TarArchiveInputStream): Iterator[TarArchiveEntry] = new Iterator[TarArchiveEntry]() {

    var curr: TarArchiveEntry = _

    override def hasNext: Boolean = {
      curr = tarArchiveInputStream.getNextTarEntry
      curr != null
    }

    override def next() = curr

  }

  private def getPrependIterator[A](first: A, iterator: Iterator[A]): Iterator[A] = new Iterator[A]() {
    var curr: A = first

    override def hasNext: Boolean = curr != null

    override def next(): A = {
      val res = curr
      if (iterator.hasNext)
        curr = iterator.next()
      else
        curr = null.asInstanceOf[A]
      res
    }

  }

}
