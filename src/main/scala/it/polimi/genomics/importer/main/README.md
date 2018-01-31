# Console Manual
To run GMQLImporter is done by command line by invoking the following line:

(Assuming the compiled Scala code is in GMQLImporter.jar)

java –jar GMQLImporter.jar configuration_xml_path gmql_conf_folder

where configuration_xml_path is the location for the configuration file and gmql_conf_folder contains the path to the folder with corresponding variables to start GMQLRepository service.

If log added at the end, it will show the number of executions already performed and stored in the database.

If added log –n where n is the n-th past execution, the statistical summary for that run is shown. Also multiple runs can be requested at the same time by separating them with comma. As example log -1, 2, 3 will show (if they exist) the last execution’s statistics, plus the statistics for the 2 executions done before, it notifies also 

If a number is not valid and will not show that specific information if so (as example: if 5 executions are already run, using log -6 will not show statistics but will tell the user that run does not exist).

If added –retry GMQLImporter tries to download the failed files on the local datasets.
