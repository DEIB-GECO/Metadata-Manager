package it.polimi.genomics.importer.ModelDatabase.Utils


object Statistics {

  var fileNumber: Int = _
  var archived: Int = _
  var released: Int = _
  var releasedItemNotInserted: Int =_
  var itemInserted: Int = _
  var itemUpdated: Int = _
  var tsvFile: Int = _
  var constraintsViolated: Int = _
  var indexOutOfBoundsException: Int = _
  var malformedInput: Int = _
  var anotherInputException: Int = _
  var correctExportedFile: Int = _
  var errorExportedFile: Int =_

  var extractTimeAcc: Long = _
  var loadTimeAcc: Long = _
  var trasformerTimeAcc: Long = _
}
