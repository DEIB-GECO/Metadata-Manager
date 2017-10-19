# GMQLImporter
GMQLImporter is the tool we use in the GMQL project to systematically import datasets from publicly available databanks towards its integration for GMQL querying. To handle any type of data source, the process of importation has been abstracted into a general structure that can fit potentially any dataset available and import it as a GDM dataset into GMQL. GMQLImporter receives a configuration file with the needed information to perform the import. The tool uses a database where records the configuration files used when running and allows easy control of changes in the GMQL repository. 

The overall process is divided in 4 main parts:
1. __Initialization__

  GMQLImporter reads its configuration file, gets the sources and datasets that are going to be imported and it registers them into the database. This process is general for any source.
  
2. __Download__

  In this step the connection to the source’s server is carried out, allowing the download of
the files that compose the desired dataset. The download has to ensure only needed files are downloaded, in short, not to download the same file 2 times when updating the local copy of the data and to know which files are outdated or the ones that are no longer used in the source’s datasets. For this management, the source should provide ideally with file size, file last modification date and the file’s hash but even with one of those attributes the updating process could be done.
Download procedure depends more in the source than the dataset itself, therefore different download methods can be implemented for different sources.

3. __Transform__

  Transformation in this context means to modify the data to be queried seamlessly not caring about the source’s differences, an important step for the correct transformation is done at metadata level, where the metadata names have to be standardized by modifying them if needed. Following this logic, the metadata from different sources could be used equally when the real attribute represented by it is the same or can be related. GMQLImporter gives the general tools for the correct transformation of the region data and their metadata. First ensures the genomic data and their metadata are in a GDM compatible format therefore performs if needed modification of the original sources’ files into GDM format. Once the NGS data and metadata are GDM friendly the transformation process checks for the region data to fulfill the dataset schema and the metadata to be revised and modified if needed for importing it. The transformation into GDM depends in the source’s original files thus different transformation to GDM methods can be implemented, while the checking for schema file, and standardization of metadata or region data are done always for GDM files. The transformation process is divided then in the step to transform into GDM and after the standardization of the files.
  
4. __Load__

  This is the final step of the overall process, and it uses the interface from GMQLRepository to import data into GMQL to be queried. This part always loads GDM data files into GMQLRepository, and therefore is generic for every source or dataset.

## How to implement your own modules into GMQLImporter

### 1. Defining XML file

GMQLImporter is designed to receive a configuration XML file with the needed parameters to perform the initialization, downloading, transforming and loading steps of the datasets, to provide a general approach for the formal creation of this file; an XSD schema file is designed to validate any configuration XML file given as input for the tool. The schema comprehends a root node where general settings and a source list are stored. Sources as seen before, represent NGS data providers which provide those genomic data and experimental metadata divided in datasets, each source contains a list of datasets, each dataset after processing, represents a GDM dataset where every sample has a region data file and a metadata and every sample share the same region data schema. The configuration XSD is organized in a tree structure starting from the root node, passing through the sources and ending in the datasets as shown below:
<img src="https://github.com/DEIB-GECO/GMQL-Importer/blob/master/src/main/scala/it/polimi/genomics/importer/GMQLImporter/Photos/XSD%20root%20node.png"      title="Root Node"/>

* __root__: contains general settings and a list for sources to import.
* __settings__: general settings for the program execution.
  * __base_working_directory__: folder where the importer will use during execution.
  * __download_enabled__: indicates if download process will be executed.		
  * __transform_enabled__: indicates if transformation process will be executed.		
  * __load_enabled__: indicates if loading process will be executed.			
  * __parallel_execution__: indicates if the whole execution is run in single thread processing or multi-thread processing.	
* __source_list__: collection of sources to be imported.
<img src="https://github.com/DEIB-GECO/GMQL-Importer/blob/master/src/main/scala/it/polimi/genomics/importer/GMQLImporter/Photos/XSD%20source%20node.png"      title="Source Node"/>

  * __source__: represents an NGS databank, contains basic information for downloading, transforming and loading process.
  * __name__: identification for the source.		
  * __url__: address of the source.
  * __source_working_directory__: 	sub directory where the source’s files will be processed.
  * __downloader__: indicates the downloading process to be performed to download the samples from this source.
  * __transformer__: indicates the transformation process to be performed to change the source samples into GDM compatible files for interoperability.		
  * __loader__: indicates the responsible for loading the processed data into a GDM 		repository.		
  * __download_enabled__: indicates if this source is going to be downloaded from the source.	
  * __transform_enabled__: indicates if transformation process is executed for this source.	
  * __load_enabled__: indicates if loading into GDM repository is executed for this source.	
  * __parameter_list__: collection of parameters for downloading or loading the source.	
  * __dataset_list__: 	collection of datasets to import from the source.
  <img src="https://github.com/DEIB-GECO/GMQL-Importer/blob/master/src/main/scala/it/polimi/genomics/importer/GMQLImporter/Photos/XSD%20dataset%20node.png"      title="Dataset Node"/>
  
  * __dataset__: represents a set of samples that share the same region data schema and the same types of experimental or clinical metadata.
  * __name__:	identifier for the dataset.
  * __dataset_working_directory__: subfolder where the download and transformation of this dataset is performed.		
  * __schema_url__: address where the schema file can be found.		
  * __schema_location__: indicates whether the schema is located in FTP, HTTP or LOCAL destination.		
  * __download_enabled__: indicates if the download process will be performed for this dataset.	
  * __transform_enabled__: indicates if the transformation process will be performed for this dataset.		
  * __load_enabled__: indicates if the loading process will be executed for this dataset.		
  * __parameter_list__: list of dataset specific parameters for downloading, transforming or loading this dataset.
  <img src="https://github.com/DEIB-GECO/GMQL-Importer/blob/master/src/main/scala/it/polimi/genomics/importer/GMQLImporter/Photos/XSD%20parameter%20node.png"      title="Parameter Node"/>
  
  * __parameter__: defines specific information for a source or a dataset, this information is useful for downloading, transforming or loading procedures.	
  * __key__: is the name for the parameter, its identifier.
  * __value__: parameter information.
  * __description__: explains what the parameter is used for.
  * __type__: optional tag for the parameter.
  
  The xml file is used to setup the complete importing procedure, during the initialization of GMQLImporter, the xml file is read and used to gather the corresponding downloaders and transformers to get and deliver the datasets requested in GDM format. The sources to be imported have to be in the xml file with the same structure of datasets that requested for GMQL. this sources with the datasets are stored in in the GMQLImporter’s database and from there the downloading, transform and load processes acquire the required parameters to perform the correct import. Newer modules can be added and different parameters will emerge in the future as other sources and datasets are added to the project.
### 2. File Database

GMQLImporter manages every file based in the records stored in its database, each file, either downloaded or transformed have to be indexed and correctly labeled. The possible status of every file can be:
* Updated: the local file is the last version available in the origin at the time of the last time GMQLImporter was run.
* Failed: the local file failed to correctly download or transform.
* Outdated: the local file does not exist anymore in the origin.
* Compare: temporary status used at runtime for comparing which files have to be outdated.
The possible status of every file are updated, compare, failed or outdated. And files can change status with the provided methods to index and keep updated the database:
<img src="https://github.com/DEIB-GECO/GMQL-Importer/blob/master/src/main/scala/it/polimi/genomics/importer/GMQLImporter/Photos/File%20status%20diagram.png"      title="File status diagram"/>

* MarkToCompare: by receiving a dataset, the database changes the status of every file in the dataset to compare, this method is used to notice which files are no longer in the server side and have to be marked after as outdated in the local copy.
* MarkAsUpdated: indicated the file was correctly downloaded or transformed and it is ready for the next step that could be transform or load.
* MarkAsFailed: if during download or transform, the procedure fails, the file has to be excluded from further processing and thus is marked as failed.
* MarkAsOutdated: once the whole dataset is downloaded or transformed, this procedure finds the files that are no longer in the server (the files that at the end of the process remain as to compare) and marks those local files as outdated, those files are excluded from the next procedure.

### 3. Downloader

If the download method needed for getting the files from a source is not available, a GMQLDownloader module can be implemented and integrated with the solution, and by giving corresponding parameters for the download, the GMQLImporter tool can get newer or better versions of the downloading procedures. The goal of the download section is to get all the needed files from the source to import the requested datasets. For developing a GMQLDownloader class some requirements have to be achieved in order to correctly integrate it into GMQLImporter:
You must import it.polimi.genomics.importer.FileDatabase.{FileDatabase, STAGE} and develop your GMQLDownloader implementing all the following steps in the “download” and “downloadFailedFiles methods inherited

  * __Get source id__
  
     val sourceId = FileDatabase.sourceId(source.name)
     val stage = STAGE.DOWNLOAD
     
  * __Get dataset id__
  
     for(dataset in source)
        val datasetId = FileDatabase.datasetId(sourceId,dataset.name)
        
  * __Mark dataset for comparison__ (only in download and not in download failed files)
  
        FileDatabase.markToCompare(datasetId,stage)
        
  * __Get file id and real filename__
  
     for(file in dataset)
     
		      val fileId = FileDatabase.fileId(datasetId, url,stage, candidateName)
        val fileNameAndCopyNumber = FileDatabase.getFileNameAndCopyNumber(fileId)
        val filename = if (fileNameAndCopyNumber._2 == 1)
           fileNameAndCopyNumber._1
           Else
              fileNameAndCopyNumber._1.replaceFirst("\\.", "_" +fileNameAndCopyNumber._2 + ".")
              
  * __Check if update the file__
  
		if (FileDatabase.checkIfUpdateFile(fileId,md5sum,fileSize,lastUpdate))
  
  * __Mark file as updated or failed__
  
  if(downloadOK)
     FileDatabase.markAsUpdated(fileId, localFileSize)
  else
     FileDatabase.markAsFailed(fileId)
     
  * __Mark dataset outdated files__ (only in download and not in download failed files)
  
     FileDatabase.markAsOutdated(datasetId,stage)

### 4. Transformer

If the transform method needed for transforming original file format into bed is not available, a GMQLTransformer module can be implemented and integrated with the solution, and by giving corresponding parameters for the transformation, the GMQLImporter tool can get newer or better versions of the transforming procedures. The goal of the transform section is to change the files format into bed format for erevy file in the requested datasets. 
GMQLImporter gives the GMQLTransformer trait that holds the methods transform and getCandidateNames that have to implemented to integrate correctly into GMQLImporter. The method transform must transform one file from the downloaded files into one bed file. For managing multiple files that are created from a single file, the method getCandidateNames must be implemented, this method gets the original file name and then must return all the files that output from it.
You must import it.polimi.genomics.importer.FileDatabase.{FileDatabase, STAGE} and develop your GMQLTransformer implementing all the following steps in the “transform” and “getCandidateNames” methods inherited:

* __getCandidateNames__

Method receives the file name, the dataset where it belongs and the source of the dataset, and must return a List[String] with the names of the files that output from the input file.

* __transform__

By receiving the source, original path, destination path, original file name and destination file name must transform the original file located in the original path into a bed file with destination name in destination path and return a boolean value indicating wether the transformation was done correctly. The FileDatabase file status management is done in the generic step of the transformation.
