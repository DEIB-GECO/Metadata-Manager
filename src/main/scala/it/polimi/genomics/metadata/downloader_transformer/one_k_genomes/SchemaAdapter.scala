package it.polimi.genomics.metadata.downloader_transformer.one_k_genomes

import java.io.BufferedReader

import it.polimi.genomics.metadata.util.{FileUtil, RoughReadProgress}
import it.polimi.genomics.metadata.util.vcf.VCFMutation
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success}

/**
 * Created by Tom on ott, 2019
 */
object SchemaAdapter {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val MISSING_VALUE_CODE = "*"


  def TESTcommonInfoAttributesPerFile(inputFiles: List[String]): List[(String, Set[String])] = {
    import it.polimi.genomics.metadata.util.vcf.VCFInfoKeys._

    inputFiles.map(inputFile =>
      (inputFile, {
        val reader = FileUtil.open(inputFile).get
        val firstMutation = advanceAndGetFirstMutation(reader)
        var setA = new VCFMutation(firstMutation).info.keySet

        FileUtil.scanFileAndClose(reader, mutationLine => {
          val m = new VCFMutation(mutationLine)
          val setB = m.info.keySet
          val notInBSet = setA.diff(setB)
          setA = setA.diff(notInBSet)
        }, setupReadProgressCanary(inputFile))
        println(inputFile+" has common attrs "+setA)
        setA
      })
    )
  }

  def TESTcommonAttributesOverall(inputFiles: List[String], output: String): ListBuffer[String] ={
    val perFileCommonAttrs = TESTcommonInfoAttributesPerFile(inputFiles)
    var allAttrs = perFileCommonAttrs.head._2.toList.to[ListBuffer]
    var commonAttrs = allAttrs.clone()
    val writer = FileUtil.writeReplace(output).get

    perFileCommonAttrs.foreach(file => {
      val perFileAttrs = file._2.toList
      // add new attributes to the list of all attrs
      val newAttrs = allAttrs.diff(perFileAttrs)
      allAttrs ++= newAttrs
      // write attributes of this file
      writer.write(file._1+",\t")
      allAttrs.foreach(attr => {
        if(perFileAttrs.contains(attr))
          writer.write(attr)
        else
          writer.write("*")
        writer.write(",\t")
      })
      writer.write("\n")
      // update common attrs list
      commonAttrs --= newAttrs
    })
    writer.close()
    println("common attributes to files: "+commonAttrs)
    commonAttrs
  }


  ////////////////      REMOVE THIS   ///////////////////
  def advanceAndGetHeaderLine(reader: BufferedReader): String = {
    var headerLine = reader.readLine()
    while (headerLine.startsWith("##")) {
      headerLine = reader.readLine()
    }
    headerLine
  }

  def advanceAndGetFirstMutation(reader: BufferedReader): String ={
    var firstVariantLine = reader.readLine()
    while(firstVariantLine.startsWith("#")) {
      firstVariantLine = reader.readLine()
    }
    firstVariantLine
  }


  private def setupReadProgressCanary(fullFilePath: String): Option[RoughReadProgress] ={
    println("COUNTING LINES OF FILE: "+fullFilePath)
    FileUtil.countLines(fullFilePath) match {
      case Failure(_) =>
        println("COUNT OF LINES IN FILE FAILED")
        None
      case Success(value) =>
        Some(new RoughReadProgress(value, 10, RoughReadProgress.notifyProgress))
    }
  }

}
