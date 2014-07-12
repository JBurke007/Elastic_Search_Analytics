Elastic_Search_Analytics
========================

Various automation scripts for the use of elasticsearch in data analytics

These scripts are used to create and manage indexes within elasticsearch and allow a user to feed the program a data schema file which creates and pushes a mapping file to elasticsearch which is used to store the data that you wish to index in elasticsearch. The script then creates a seperate load file which is inteneded to be used to load the raw data that you have, into elasticsearch.

The user will be prompted for a number of key pieces of information such as; does the data have a date and time column, or what is the delimiter for your data. There are various log files created with these scripts that identify errors with the loading or creating of map files and indexes. One more important prompt for the user, is to select the encoding that the raw data is coded with, this helps the script to know what type of data that it is loading into elasticsearch which reduces the chances of any foreign languauge text being left out of the upload.
