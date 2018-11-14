package it.polimi.genomics.metadata.cleaner

import java.io.File

object Utils {

  def getListOfSubDirectories(directoryName: String): Array[String] = {
    (new File(directoryName))
      .listFiles
      .filter(_.isDirectory)
      .map(_.getName)
  }

  def getListOfMetaFiles(dir: File): Array[File] = {
    val filesList: Array[File] = dir.listFiles
    //println(dir + "---------" + filesList)
    val res = filesList ++ filesList.filter(_.isDirectory).flatMap(getListOfMetaFiles)
    res.filter(_.getName.endsWith(".meta"))
  }

  def getListOfRegFiles(dir: File): Array[File] = {
    val filesList: Array[File] = dir.listFiles
    //println(dir + "---------" + filesList)
    val res = filesList ++ filesList.filter(_.isDirectory).flatMap(getListOfMetaFiles)
    res.filter(_.getName.endsWith(".bed"))
  }

  def buildRulePair(input_string: String): (String, String) = {
    val rule_pattern = "(.*)=>(.*)"
    (input_string.replaceAll(rule_pattern, "$1"), input_string.replaceAll(rule_pattern, "$2"))
  }

  def extractPair(input_line: String): (String, String) = {
    val key_pattern = "(.*)(\\t)(.*)"
    val key = input_line.replaceAll(key_pattern, "$1")
    val value = input_line.replaceAll(key_pattern, "$3")
    (key, value)
  }

  def extractKey(input_line: String): String = {
    val key_pattern = "(.*)(\\t)(.*)"
    val key = input_line.replaceAll(key_pattern, "$1")
    key
  }

  def rebuildLine(input_pair: (String, String)): String = {
    val line = input_pair._1 + "\t" + input_pair._2
    line
  }

}
