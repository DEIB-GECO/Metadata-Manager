package it.polimi.genomics.metadata.downloader_transformer.one_k_genomes

import java.io.BufferedReader

import it.polimi.genomics.metadata.util.vcf.VCFMutation
import it.polimi.genomics.metadata.util.{FileUtil, RoughReadProgress}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success}

/**
 * Created by Tom on ott, 2019
 */
object SchemaAdapter {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val MISSING_VALUE_CODE = "*"


  def TESTcommonInfoAttributesPerFile(inputFiles: List[String]): List[(String, Set[String])] = {

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

  def TESTcommonAttributesOverall(inputFiles: List[String], output: String): Set[String] ={
    var allAttrs = Set.empty[String]
    val perFileCommonAttrs = inputFiles.map(inputFile =>
      (inputFile, {
        val reader = FileUtil.open(inputFile).get
        val firstMutation = advanceAndGetFirstMutation(reader)
        var setA = new VCFMutation(firstMutation).info.keySet
        // add new attributes to the list of all attrs
        allAttrs ++= setA

        FileUtil.scanFileAndClose(reader, mutationLine => {
          val m = new VCFMutation(mutationLine)
          val setB = m.info.keySet
          // add new attributes to the list of all attrs
          allAttrs ++= setB
          // update set of common attrs in file
          val notInBSet = setA.diff(setB)
          setA = setA.diff(notInBSet)
        }, setupReadProgressCanary(inputFile))
        println(inputFile+" has common attrs "+setA)
        setA
      })
    )

    val writer = FileUtil.writeReplace(output).get
    val header = "file,\t"+allAttrs.reduce((at1, at2) => at1+",\t"+at2)
    writer.write(header)
    writer.newLine()

    var commonOverallAttrs:Set[String] = allAttrs.map(elem => elem)
    perFileCommonAttrs.foreach(file => {
      // write filename
      writer.write(DatasetInfo.parseFilenameFromURL(file._1)+",\t")
      // write attributes of this file
      val perFileAttrs = file._2
      val newAttrs = commonOverallAttrs.diff(perFileAttrs)
      allAttrs.foreach(attr => {
        if(perFileAttrs.contains(attr))
          writer.write(attr)
        else
          writer.write("*")
        writer.write(",\t")
      })
      writer.newLine()
      // update common attrs list
      commonOverallAttrs --= newAttrs
    })
    writer.close()
    println("common attributes to files: "+commonOverallAttrs)
    commonOverallAttrs
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
