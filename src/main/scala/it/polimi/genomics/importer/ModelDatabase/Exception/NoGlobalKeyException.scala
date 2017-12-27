package exceptions

case class NoGlobalKeyException (val error: String) extends Exception{
  private val _message = error

  // Getter
  def message = _message

}
