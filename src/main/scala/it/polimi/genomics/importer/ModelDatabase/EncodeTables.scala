package it.polimi.genomics.importer.ModelDatabase


import main.{name, tables}

class EncodeTables extends Tables{
  /*val bioSample = new BioSample
  val cases = new Case
  val caseItem = new CaseItem
  val container = new Container
  val donor = new Donor
  val experimentType = new ExperimentType
  val item = new Item
  val project = new Project*/

  //private val tables = collection.mutable.Map[String, AbstractTable]()
  EncodeTableEnum.values.foreach(v => tables += v ->EncodeTableEnum.getTable(v))


  /*tables += ("BIOSAMPLES" -> bioSample)
  tables += ("CASES" -> cases)
  tables += ("CASESITEMS" -> caseItem)
  tables += ("CONTAINERS" -> container)
  tables += ("DONORS" -> donor)
  tables += ("EXPERIMENTSTYPE" -> experimentType)
  tables += ("ITEMS" -> item)
  tables += ("PROJECTS" -> project)*/



  def getDonorTable(): Donor = {
    tables(EncodeTableEnum.Donors).asInstanceOf[Donor]
  }
}
