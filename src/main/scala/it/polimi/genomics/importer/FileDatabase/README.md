# File Database

GMQLImporter manages every file based in the records stored in its database, each file, either downloaded or transformed have to be indexed and correctly labeled. The possible status of every file can be:
* Updated: the local file is the last version available in the origin at the time of the last time GMQLImporter was run.
* Failed: the local file failed to correctly download or transform.
* Outdated: the local file does not exist anymore in the origin.
* Compare: temporary status used at runtime for comparing which files have to be outdated.
The possible status of every file are updated, compare, failed or outdated. And files can change status with the provided methods to index and keep updated the database:
<img src="https://github.com/DEIB-GECO/GMQL-Importer/blob/master/src/main/scala/it/polimi/genomics/importer/GMQLImporter/Photos/File%20status%20diagram.png"      title="File status diagram"/>

* __MarkToCompare__: by receiving a dataset, the database changes the status of every file in the dataset to compare, this method is used to notice which files are no longer in the server side and have to be marked after as outdated in the local copy.
* __MarkAsUpdated__: indicated the file was correctly downloaded or transformed and it is ready for the next step that could be transform or load.
* __MarkAsFailed__: if during download or transform, the procedure fails, the file has to be excluded from further processing and thus is marked as failed.
* __MarkAsOutdated__: once the whole dataset is downloaded or transformed, this procedure finds the files that are no longer in the server (the files that at the end of the process remain as to compare) and marks those local files as outdated, those files are excluded from the next procedure.
