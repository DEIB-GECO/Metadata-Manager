# GMQL-Importer [![Build Status](https://travis-ci.org/DEIB-GECO/GMQL-Importer.svg?branch=master)](https://travis-ci.org/DEIB-GECO/GMQL-Importer)

## Synopsis

[GMQL-Importer](https://github.com/DEIB-GECO/GMQL-Importer) gathers information from heterogeneous datasources and integrates them into the [Genomic Data Model (GDM)](http://www.sciencedirect.com/science/article/pii/S1046202316303012) for further processing using [GenoMetric Query Language (GMQL)](http://www.bioinformatics.deib.polimi.it/genomic_computing/)

## Motivation

[GMQL-Importer](https://github.com/DEIB-GECO/GMQL-Importer) is meant to be the manager for the [GMQL-Repository](https://github.com/DEIB-GECO/GMQL/tree/master/GMQL-Repository) and integrate the different sources used in the [GMQL Project](https://github.com/DEIB-GECO/GMQL), these different sources can have many data formats and procedures to obtain them

## Repository Structure

* [GMQLImporter source](https://github.com/DEIB-GECO/GMQL-Importer/tree/master/src/main/scala/it/polimi/genomics/importer)
   * [GMQLImporter](https://github.com/DEIB-GECO/GMQL-Importer/tree/master/src/main/scala/it/polimi/genomics/importer/GMQLImporter) contains the base components for defining the required implementation to correctly run download, transformation and loading of datasets. Correctness of the configuration files is also managed here.
   * [FileDatabase](https://github.com/DEIB-GECO/GMQL-Importer/tree/master/src/main/scala/it/polimi/genomics/importer/FileDatabase) records and manages the status of downloaded, transformed and loaded files, tracking their history and checking their correctness.
   * [DefaultImporter](https://github.com/DEIB-GECO/GMQL-Importer/tree/master/src/main/scala/it/polimi/genomics/importer/DefaultImporter) includes basic examples of downloaders and transformers.
   * [ENCODEImporter](https://github.com/DEIB-GECO/GMQL-Importer/tree/master/src/main/scala/it/polimi/genomics/importer/ENCODEImporter) specific implementation for integrating ENCODE's datasets.
   * [CistromeImporter](https://github.com/DEIB-GECO/GMQL-Importer/tree/master/src/main/scala/it/polimi/genomics/importer/CistromeImporter) project for integration of Cistrome's datasets into GMQL.
   * [main](https://github.com/DEIB-GECO/GMQL-Importer/tree/master/src/main/scala/it/polimi/genomics/importer/main) console application for user interface.

* [Examples](https://github.com/DEIB-GECO/GMQL-Importer/tree/master/Example) contains multiple testing scenarios for the configuration files needed to run GMQLImporter.

## Usage and contribution

The full [GMQL Project](https://github.com/DEIB-GECO/GMQL) can be pulled from the GitHub repository, installation of its dependencies is done by [Apache Maven](https://maven.apache.org/).For correctly deployment of a fully functional GMQL server for developing in GMQLImporter, follow the [GMQL Local deployment for developers Guide](https://docs.google.com/document/d/14ZnvL7vMJHZY5sNy3lcP-HidlCdxcLPfIwCD2nBAxvc/edit?usp=sharing) (revision needed). For implementing your own downloaders and transformers to integrate new sources, follow the [Welcome to GMQLImporter for developers](https://docs.google.com/document/d/10A0fS6j4yp252rJdFiQUUkAMjyPgvo0M08dfFVKe8bU/edit?usp=sharing) guide.

## Example

Once you have downloaded and installed [GMQL Project](https://github.com/DEIB-GECO/GMQL) inside the [Source Folder](https://github.com/DEIB-GECO/GMQL-Importer/tree/master/src/main/scala/it/polimi/genomics/importer), there is an [example](https://github.com/DEIB-GECO/GMQL-Importer/tree/master/Example) to run and check the basic functionallity of GMQL-Importer

## Contributors
|           | username                                           | talk to me about...                               |
|-----------|----------------------------------------------------|---------------------------------------------------|
|<img src="https://avatars.githubusercontent.com/acanakoglu"      height="50px" title="Arif Canakoglu"/>        | [`@acanakoglu`](https://github.com/acanakoglu)           | Supervisor for the design and development of GMQLImporter |
|<img src="https://avatars.githubusercontent.com/nachodox"      height="50px" title="Nacho Vera"/>        | [`@NachoVera`](https://github.com/nachodox)           | Anything related to the GMQL-Importer for now |
|<img src="https://avatars.githubusercontent.com/federicogatti"      height="50px" title="Federico Gatti"/>        | [`@federicogatti`](https://github.com/federicogatti)           | Federico's role |
