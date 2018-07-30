# Source Code Structure


* [database](./database) contains a module that records and manages the status of the files for each level, tracks the history of the previous execution of the system.
* [step](./step) contains the base components for defining the required implementation to correctly run each step. The configuration XML related files are also available in this folder
* [downloader_transformer](./downloader_transformer) includes basic examples of _Downloader_ and _Transformer_ classes and also specific implementation for different data sources. 
* [cleaner](./cleaner) contains the cleaner module's source codes
* [mapper](./mapper) contains the mapper module's source codes