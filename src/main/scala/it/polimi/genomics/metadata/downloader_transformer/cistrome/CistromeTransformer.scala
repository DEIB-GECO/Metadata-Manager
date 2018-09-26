package it.polimi.genomics.importer.CistromeImporter

import java.io._

import it.polimi.genomics.metadata.downloader_transformer.Transformer
import it.polimi.genomics.metadata.step.xml.{Dataset, Source}
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveInputStream}
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.io.IOUtils
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.io.BufferedSource
import scala.util.matching.Regex


class CistromeTransformer extends Transformer {
  val logger = LoggerFactory.getLogger(this.getClass)

  val metadataDictionary = collection.mutable.Map.empty[String, ListBuffer[Tuple2[String, String]]]


  val headerCistrome = List("id", "geo_id", "organism", "cell_line", "cell_type", "tissue_origin", "histone_mark", "name")
  val headerOnassis = List("geo_id", "term_id", "term_name", "term_url", "matched_text", "type", "name")

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
        val keyValue = metadataDictionary(filename)
        val writer = new PrintWriter(fileTransformationPath)

        keyValue.foreach(t =>
          writer.write(t._1 + "\t" + t._2 + "\n")
        )

        writer.close()
        //correct this function and set as true
        true
      }
      else {
        val inputStream = untarListRecursively(fileDownloadPath, Some(filename)).right.get
        println("filename: [" + filename + "]")
        println("fileTransformationPath: [" + fileTransformationPath + "]")
        IOUtils.copy(inputStream, new FileOutputStream(fileTransformationPath))
        if (filename.equals("1028_b_sort_peaks.broadPeak.bed"))
          println("hello")
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
      case fileRegex(fileName) =>
        Some(fileName)
      case metadataFileRegex(metadataFileName) =>
        val metadataStream = untarListRecursively(path, Some(metadataFileName)).right.get
        val bs = new BufferedSource(metadataStream)

        val lineList: Iterator[String] = bs.getLines

        val onassisFilePath = dataset.getParameter("onassis_file_path").getOrElse("dummy")
        metadataDictionary ++=
          fillDictionary(lineList, onassisFilePath, fileRegex)
            //select only related peak type samples and then add ".meta" at the end
            .filter(t => fileRegex.findFirstMatchIn(t._1).isDefined)
            .map { t => (t._1 + ".meta", t._2) }

        metadataDictionary.keys
      case _ =>
        None
    }

    relatedFiles
      //      .map({ t => println(t); t })
      //      .filter(_.contains("1028"))
      .toList

  }

  //Implemented by Michele Leone
  def fillDictionary(metadataFileLines: Iterator[String], onassisFilePath: String, fileRegex: Regex) = {
    val metadataDictionaryTemp = collection.mutable.Map.empty[String, ListBuffer[Tuple2[String, String]]]

    for (line <- metadataFileLines) {
      val values2 = line.split("\t")
      val name2 = values2(7)

      if (metadataDictionaryTemp.contains(name2)) {
        val new_dict = metadataDictionaryTemp(name2)
        for (i <- headerCistrome.indices) {
          new_dict += ((headerCistrome(i), values2(i)))
        }
        metadataDictionaryTemp(name2) = new_dict.sortBy(_._1)
      }
      else {
        var new_list: ListBuffer[(String, String)] = ListBuffer()
        for (i <- headerCistrome.indices) {
          new_list += ((headerCistrome(i), values2(i)))
        }
        metadataDictionaryTemp.put(name2, new_list.sortBy(_._1))
      }
    }

    // Read once if exists
    if (new java.io.File(onassisFilePath).exists) {
      val onassisLines = scala.io.Source.fromFile(onassisFilePath).getLines
      for (line <- onassisLines) {
        val values = line.split("\t")
        val name = values(6)
        val type_term = (values(5), values(2))

        if (metadataDictionaryTemp.contains(name)) {
          val new_dict = metadataDictionaryTemp(name)
          for (i <- headerOnassis.indices) {
            new_dict :+ ((headerOnassis(i), values(i)))
          }
          new_dict += type_term
          metadataDictionaryTemp(name) = new_dict.sortBy(_._1)
        }
        else {
          var new_list: ListBuffer[(String, String)] = ListBuffer()
          for (i <- headerOnassis.indices) {
            new_list += ((headerOnassis(i), values(i)))
          }
          new_list += type_term
          metadataDictionaryTemp.put(name, new_list.sortBy(_._1))
        }
      }

    }
    else
      logger.warn("Onassis file is not available")


    for ((name, v) <- metadataDictionaryTemp) {
      val tissue_list = new ListBuffer[String]()
      val disease_list = new ListBuffer[String]()
      for (i <- v) {
        if (i._1 == "tissue")
          tissue_list += i._2.toLowerCase()
        else if (i._1 == "disease")
          disease_list += i._2.toLowerCase()
      }


      val tissue_list_filtered = tissue_list.distinct.sorted -= "none"
      disease_list.distinct.sorted

      if (tissue_list.nonEmpty) {
        var semantic_annotation = ""
        for (tissue <- tissue_list_filtered) {
          semantic_annotation += tissue + "_"
        }
        if (disease_list.nonEmpty) {
          semantic_annotation = semantic_annotation.dropRight(1) + "_/_"
        }
        for (disease <- disease_list) {
          semantic_annotation += disease + "_"
        }
        semantic_annotation = semantic_annotation.dropRight(1)
        v += (("semantic_annotation", semantic_annotation))
      }
    }

    metadataDictionaryTemp
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
