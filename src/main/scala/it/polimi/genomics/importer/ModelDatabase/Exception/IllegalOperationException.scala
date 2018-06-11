package it.polimi.genomics.importer.ModelDatabase.Exception

class IllegalOperationException (val error: String) extends Exception{
  // Getter
  def message = error

}
