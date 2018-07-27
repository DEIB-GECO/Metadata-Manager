package exceptions

case class NoSourceKeyException(val error: String) extends Exception{
  private val _message = error

  // Getter
  def message = _message
}
