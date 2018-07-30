The RoadmapImporter modules (i.e., RoadmapDownloader, RoadmapTransformer) are the GMQL-Importer modules used to download and transform the [Roadmap Epigenomics Project](http://www.roadmapepigenomics.org/) data.

## Using the modules
The RoadmapDownloader modules can be used adding the following tag in the source section of the configuration XML:
```XML
<downloader>it.polimi.genomics.importer.RoadmapImporter.RoadmapDownloader</downloader>
```
The RoadmapTransformer modules can be used adding the following tag in the source section of the configuration XML:
```XML
<transformer>it.polimi.genomics.importer.RoadmapImporter.RoadmapTransformer</transformer>
```

## Google API
The RoadmapDownloader module requires the use of Google Spreadsheet API to download some metadata. To access the services provided by the API, the application must authenticate itself using the OAuth2 protocol. In order to do so, they are required:
1. a JSON file containing the information used to authenticate the application;
2. a directory where to store the credentials obtained after the authentication.

The JSON file can be provided by using _client_secrets_path_ source parameter in the XML configuration file. 
For example, if the JSON file containing the authentication information is _/home/user/GMQL-Importer/client_secrets.json_, in the source section of the configuration XML you should add the tags:
```XML
<parameter>
  <key>client_secrets_path</key>
  <value>/home/user/GMQL-Importer/client_secrets.json</value>
</parameter>
```
It is highly recommended to use the [JSON file](https://github.com/DEIB-GECO/GMQL-Importer/blob/RoadmapImporter/client_secrets.json) provided in the project repository.
The directory used to store credentials can be set in the source section of the XML configuration file using the _credential_directory_ parameter.
For example, if you want to use the directory _/home/user/GMQL-Importer/.credentials/sheets.googleapis.com-GMQL-importer/_ to store the credentials, in the source section of the XML configuration file you have to include the tags:
```XML
<parameter>
  <key>credential_directory</key>
  <value>/home/user/GMQL-Importer/.credentials/sheets.googleapis.com-GMQL-importer/</value>
</parameter>
```
If you do not specify any JSON file using the _client_secrets_path_ source parameter, the program will search a file named _client_secrets.json_ in the [current working directory](https://en.wikipedia.org/wiki/Working_directory) of the program by default. If the JSON file is not found, the authentication process fails, since the Google API is not able to identify the application that makes the access request to its services.
If you do not specify any path where to store the credentials using the _credential_directory_ parameter, the program will use the [current working directory](https://en.wikipedia.org/wiki/Working_directory) of the program by default. Conversely, if the specified directory does not exist, the authentication process fails. If in the specified credentials directory there are already some credentials, those credentials will be used. Otherwise, a login page is automatically opened in the default Web browser and you have to log in using a valid Google account. Credentials expire periodically and they are automatically replaced without the need to log in again with a Google account. The credentials are stored in a file called _StoredCredential_. If you are running the program in an environment with no Web browser, you can use the [credentials](https://github.com/DEIB-GECO/GMQL-Importer/blob/RoadmapImporter/.credentials/sheets.googleapis.com-GMQL-importer/StoredCredential) provided in the repository.

