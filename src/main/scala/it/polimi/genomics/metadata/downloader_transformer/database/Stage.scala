package it.polimi.genomics.metadata.downloader_transformer.database

/**
  * Created by Nacho on 12/1/16.
  * Represents the stage of the process
  */
object Stage extends Enumeration {
  type STAGE = Value
  val DOWNLOAD = Value("DOWNLOAD")
  val TRANSFORM = Value("TRANSFORM")
  val CLEAN = Value("CLEAN")
}
