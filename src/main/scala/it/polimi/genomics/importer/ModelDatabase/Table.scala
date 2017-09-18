package it.polimi.genomics.importer.ModelDatabase


trait Table {
  def setParameter(param: String, dest: String): Unit
 // def setValue(actualParam: String, newParam: String): String
  def checkInsert():Boolean
  def insert(): Int
}
