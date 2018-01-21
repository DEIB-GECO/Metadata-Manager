package it.polimi.genomics.importer.ModelDatabase.Utils

import java.io.File

object ListFiles {

  def recursiveListFiles(f: File): Array[File] = {
    val these = f.listFiles
    these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
  }

}
