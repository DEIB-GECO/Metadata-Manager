package it.polimi.genomics.metadata.mapper.Utils


object Statistics {

  var fileNumber: Int = _
  var archived: Int = _
  var released: Int = _
  var discarded: Int = _
  var releasedItemNotInserted: Int =_
  var itemInserted: Int = _
  var itemUpdated: Int = _
  var donorInsertedOrUpdated: Int = _
  var biosampleInsertedOrUpdated: Int = _
  var replicateInsertedOrUpdated: Int = _
  var ancestryInsertedOrUpdated: Int = _
  var tsvFile: Int = _
  var constraintsViolated: Int = _
  var indexOutOfBoundsException: Int = _
  var malformedInput: Int = _
  var anotherInputException: Int = _
  var correctExportedFile: Int = _
  var errorExportedFile: Int =_

  var extractTimeAcc: Long = _
  var loadTimeAcc: Long = _
  var transformTimeAcc: Long = _

  def incrementExtractTime(time: Long): Unit = {
    extractTimeAcc += time
  }

  def incrementLoadTime(time: Long): Unit = {
    loadTimeAcc += time
  }

  def incrementTrasformTime(time: Long): Unit = {
    transformTimeAcc += time
  }

  def getTimeFormatted(time:Long): String = {
    val hours = Integer.parseInt(""+(time/1000000000/60/60))
    val minutes = Integer.parseInt(""+(time/1000000000/60-hours*60))
    val seconds = Integer.parseInt(""+(time/1000000000-hours*60*60-minutes*60))
    s"$hours:$minutes:$seconds"
  }
}
