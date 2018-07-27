package it.polimi.genomics.metadata.mapper.Exception

class UniqueKeyException (val error: String) extends Exception{

  private val _message = error

  // Getter
  def message = _message
}