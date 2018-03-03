package it.polimi.genomics.importer.ModelDatabase.Exception

class NoTupleInDatabaseException (val error: String) extends Exception{

  private val _message = error

  // Getter
  def message = _message
}
