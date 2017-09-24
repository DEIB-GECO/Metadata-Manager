package it.polimi.genomics.importer.ModelDatabase


import it.polimi.genomics.importer.ModelDatabase.Utils.XMLReader

import scala.io.Source


object main extends App{

  val xml = new XMLReader("/home/federico/IdeaProjects/file copia/target/scala-2.12/classes/setting.xml")

  //creo il mapping nel file di nacho, problema degli spazi
  val lines = Source.fromFile("/Users/federicogatti/IdeaProjects/Encode_Download/HG19_ENCODE/narrowPeak/Transformations/ENCFF001UYN.bed.meta").getLines.toArray
  var states = collection.mutable.Map[String, String]()
  var tables = new EncodeTables


  for(l <- lines){
    val first = l.split(" ", 2)
    states += (first(0) -> first(1))
  }

  val operationsList = xml.operationsList

  def populateTable(list: List[String], table: Table): Unit ={
    table.setParameter(states(list(2)),list(3))
  }

 /* def setForeignKeys(table: Table): Unit = {
    table.setForeignKeys()
  }

  def insertTableIntoDatabase(table: Table): Unit ={
    if(table.checkInsert())
      table.insert()
  }*/
  //var current_table = "DONORS"
  var continue = true

  operationsList.map(x =>
    try {
      populateTable(x, tables.selectTableByName(x.head))
    } catch {
      case e: Exception => println("Source_key non trovata")
  })
  tables.insertTables()

}
