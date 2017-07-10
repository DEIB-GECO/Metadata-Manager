package it.polimi.genomics.importer.GMQLImporter.utils

/**
  * Created by Nacho on 10/14/16.
  * Defines the possible location for schemas
  */
object SCHEMA_LOCATION extends Enumeration {
  type SCHEMA_LOCATION = Value
  val LOCAL = Value("local")
  val HTTP = Value("http")
  val FTP = Value("ftp")
}
