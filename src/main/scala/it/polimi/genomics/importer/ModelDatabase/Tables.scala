package it.polimi.genomics.importer.ModelDatabase


trait Tables {

  protected val tables = collection.mutable.Map[EncodeTableEnum.Value, EncodeTable]()

  def selectTableByName(name: String): EncodeTable = tables(EncodeTableEnum.withName(name))
  def selectTableByValue(enum: EncodeTableEnum.Value): EncodeTable = tables(enum)

}
