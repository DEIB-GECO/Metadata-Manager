package it.polimi.genomics.metadata.mapper.Exception

class IllegalOperationException (val error: String) extends Exception{
  // Getter
  def message = error

}
