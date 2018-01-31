   # Importer source code
   * [GMQLImporter](https://github.com/DEIB-GECO/GMQL-Importer/tree/master/src/main/scala/it/polimi/genomics/importer/GMQLImporter) contains the base components for defining the required implementation to correctly run download, transformation and loading of datasets. Correctness of the configuration files is also managed here.
   * [FileDatabase](https://github.com/DEIB-GECO/GMQL-Importer/tree/master/src/main/scala/it/polimi/genomics/importer/FileDatabase) records and manages the status of downloaded, transformed and loaded files, tracking their history and checking their correctness.
   * [DefaultImporter](https://github.com/DEIB-GECO/GMQL-Importer/tree/master/src/main/scala/it/polimi/genomics/importer/DefaultImporter) includes basic examples of downloaders and transformers.
   * [ENCODEImporter](https://github.com/DEIB-GECO/GMQL-Importer/tree/master/src/main/scala/it/polimi/genomics/importer/ENCODEImporter) specific implementation for integrating ENCODE's datasets.
   * [CistromeImporter](https://github.com/DEIB-GECO/GMQL-Importer/tree/master/src/main/scala/it/polimi/genomics/importer/CistromeImporter) project for integration of Cistrome's datasets into GMQL.
   * [main](https://github.com/DEIB-GECO/GMQL-Importer/tree/master/src/main/scala/it/polimi/genomics/importer/main) console application for user interface.
