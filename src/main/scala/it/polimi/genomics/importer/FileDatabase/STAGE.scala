package it.polimi.genomics.importer.FileDatabase

/**
  * Created by Nacho on 12/1/16.
  * Represents the stage of the process
  */
object STAGE extends Enumeration {
  type STAGE = Value
  val DOWNLOAD = Value("DOWNLOAD")
  val TRANSFORM = Value("TRANSFORM")
  val CLEAN = Value("CLEAN")
}
