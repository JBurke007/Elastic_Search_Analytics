"""
#########################################################################################################################################
#########################################################################################################################################

		Program Title: Elasticsearch Automation Script
		Creator: James A. Burke - PwC Senior Associate - FTS - Tyson's Corner - McLean VA

		Description: This automation script takes an input file that contains the schema of the data that you want to load into elasticsearch
		and then automatically builds and loads the required mapping index into elasticsearch. It also creates a hard copy of the map index
		that is created for documentation purposes or for re-use shall you need the same index map. This program also creates a load script
		so that a user can automatically load data into elasticsearch.

		The input file that is used to build out the mapping index based on the data schema must contain 3 key pieces of information about
		the data that you want to load into elasticsearch,

		The three (3) key pieces of information that are required are;

		1.column name, 2.field type, 3.whether you want the field tokenizing or not.

		Each of the required pieces of information must be comma delimited in order for the script to accurately interpret the file

		example below

		DATETIME,date,yes
		CUSTOMER_LASTNAME,string,yes
		CUSTOMER_CITY,string,no

#########################################################################################################################################
#########################################################################################################################################
"""
import requests

print "\n Enter the details of your new project and index(es) below"

#User input variables
PROJECTNAME = raw_input ('\nEnter Project Name: ')
INDEXNAME = raw_input ('\nEnter New Index Name: ')
SCHEMAFILENAME = raw_input ('\nEnter Data Schema File Name: ')
DATETIMEINPUT = raw_input ('\nDoes the load data have a date and time field? (yes/no) ')

DATE_AND_TIME_FIELD = ''
DATETIMEFIELDNUMBER = ''

#input to decide which map script to create - depending on whether or not the data you wish to load to ES has a date and time field.
if DATETIMEINPUT == "yes":
	DATETIME = raw_input ('\nEnter Name of date and time field? ')
	DATE_AND_TIME_FIELD += DATETIME
	DATEANDTIMECOLUMNNUMBER = raw_input ('\nWhat column number is the date and time field: ')
	DATETIMEFIELDNUMBER += DATEANDTIMECOLUMNNUMBER
elif DATETIMEINPUT == "no":
	DATE_AND_TIME_FIELD = ''
else:
	print "An error occurred during input, please retry"

MAPOUTPUTFILE = raw_input ('\nEnter map output file name: ')
NAMEOFLOADFILE = raw_input ('\nEnter name for load script output: (e.g.projectname.py) ')

holdinglist = []
outputlist = []

#f = open(SCHEMAFILENAME)
#lines = f.readlines()
lines = open(SCHEMAFILENAME).readlines()

#Used to calculate the last line in the input file, so that when the mapping file is created, there is no comma assigned to the last property in the json file.
line_count = 0
row_count = 0

#creating output files to write to
OUTPUT_MAPOUTPUTFILE = open(MAPOUTPUTFILE, "w") #File to print the data to

for x in lines:
	line_count += 1

for row in lines:
	row_count +=1
	fields = row.split(",")

	if row_count != line_count:
		if "yes" not in fields[2]:
			holdinglist.append("\t\t\t"+"\""+fields[0]+"\": {\"type\": \""+fields[1]+"\", \"index\": \"not_analyzed\"},")
		elif "yes" in fields[2]:
			holdinglist.append("\t\t\t"+"\""+fields[0]+"\": {\"type\": \""+fields[1]+"\"},")
		else:
			print "something was wrong with your input file, check its layout and retry"
	elif row_count == line_count:
		if "yes" not in fields[2]:
			holdinglist.append("\t\t\t"+"\""+fields[0]+"\": {\"type\": \""+fields[1]+"\", \"index\": \"not_analyzed\"}")
		elif "yes" in fields[2]:
			holdinglist.append("\t\t\t"+"\""+fields[0]+"\": {\"type\": \""+fields[1]+"\"}")
		else:
			print "something was wrong with your input file, check its layout and retry"
	else:
		"There was something wrong with the data that you tried to use. Please check the data and try again....wamp wamp"

#adding data to the outputlist in row delimiter fashion
for items in holdinglist:
	outputlist.append(str(items)+"\n")

#Adding data to the output file
if DATETIMEINPUT == "yes":
	OUTPUT_MAPOUTPUTFILE.write("{\n")
	OUTPUT_MAPOUTPUTFILE.write("\t\""+INDEXNAME+"\": {\n")
	OUTPUT_MAPOUTPUTFILE.write("\t\"_timestamp\" : {\n \t\t\"enabled\" : true, \n \t\t\"path\" : \""+DATE_AND_TIME_FIELD+"\"\n\t\t},\n")
	OUTPUT_MAPOUTPUTFILE.write("\t\t\""+"properties"+"\": {\n")

	for items in outputlist:
	  OUTPUT_MAPOUTPUTFILE.write(str(items))

	OUTPUT_MAPOUTPUTFILE.write("\t\t}\n")
	OUTPUT_MAPOUTPUTFILE.write("\t}\n")
	OUTPUT_MAPOUTPUTFILE.write("}")
	#Close the output file
	OUTPUT_MAPOUTPUTFILE.close()
elif DATETIMEINPUT == "no":
	OUTPUT_MAPOUTPUTFILE.write("{\n")
	OUTPUT_MAPOUTPUTFILE.write("\t\""+INDEXNAME+"\": {\n")
	OUTPUT_MAPOUTPUTFILE.write("\t\t\""+"properties"+"\": {\n")

	for items in outputlist:
	  OUTPUT_MAPOUTPUTFILE.write(str(items))

	OUTPUT_MAPOUTPUTFILE.write("\t\t}\n")
	OUTPUT_MAPOUTPUTFILE.write("\t}\n")
	OUTPUT_MAPOUTPUTFILE.write("}")
	#Close the output file
	OUTPUT_MAPOUTPUTFILE.close()
else:
	"Something went wrong - start again"

#Prints out the number of rows detected in the input file
#print line_count, "rows in original file"
#print row_count

#--------------------------------------------------------------------------------------#
# The code below reads the file that was created above, and then auto loads it into ES #
#--------------------------------------------------------------------------------------#

f = open(MAPOUTPUTFILE)

mydata=(f.read())

r = requests.post("http://localhost:9200/"+PROJECTNAME+"/"+INDEXNAME+"/_mapping", data=mydata)

if r.status_code == 200:
	#print r.status_code
	#print "\n\n",r.text
	#print "\n\n",mydata
	print "#########################################################################\n"
	print "\nMapping file for index \""+INDEXNAME+"\" has been successfully loaded to elasticsearch"
elif r.status_code == 400:
	print "#########################################################################\n"
	print "Either The project for this mapping does not exist, or a mapping with the same name exists. \n Create the project first, check to see if the mapping already exists, and then re-try."
else:
	print "Index not created - make sure the index is not already in use."

#Close the output file for the final time.
f.close()

#----------------------------------------------------------------------------------#
# The code below creates the python script to load the raw data into elasticsearch #
#----------------------------------------------------------------------------------#

if DATETIMEINPUT == "yes":

	#creating output files to write data to
	LOADFILEOUTPUT = open(NAMEOFLOADFILE, "w")
	#Creating the firs section of the load file.
	LOADFILEOUTPUT.write("from pyelasticsearch import ElasticSearch \n")
	LOADFILEOUTPUT.write("import time, codecs \n\n")
	LOADFILEOUTPUT.write("correct_counter = 0"+"\n"+"failed_count = 0"+"\n"+"data = []"+"\nfailurelist = []\n\n")
	LOADFILEOUTPUT.write("conn = ElasticSearch('http://127.0.0.1:9200') \nstart = time.clock() \n\n")
	LOADFILEOUTPUT.write("input1 = raw_input ('Enter load data file name: ') \ninput2 = raw_input ('Enter Project Name: ') \ninput3 = raw_input ('Enter index name: ') \ninput4 = raw_input ('How many columns does your data have? ') \ninput5 = raw_input ('What is the delimiter? ') \ninput6 = raw_input ('Enter file name for error output (.txt) ') \ninput7 = raw_input ('Enter the codec that the data is formatted in: e.g. \\'ascii''') \n\nFILENAME = codecs.open(input1, 'r', str(input7)) \nPROJECTNAME = str(input2) \nINDEXNAME = str(input3) \nNUM_OF_FIELDS = int(input4) \nDELIMITER = str(input5) \nERRORFILEOUTPUT = open(input6, \"w\")\n\nprint \"Running...\"\n\n")
	LOADFILEOUTPUT.write("for line in FILENAME:\n\t")
	LOADFILEOUTPUT.write("fields = line.split(DELIMITER) \n\tif len(fields) == NUM_OF_FIELDS:\n\t\t")

	DTNUMBER = int(DATETIMEFIELDNUMBER)-1

	LOADFILEOUTPUT.write("d = time.strptime(fields["+str(DTNUMBER)+"].strip(),\"%Y-%m-%d %H:%M:%S\")\n\t\t")
	LOADFILEOUTPUT.write("data.append({\n")
	LOADFILEOUTPUT.write("\t\t\t\""+DATE_AND_TIME_FIELD+"\": time.strftime(\"%Y-%m-%dT%H:%M:%S\",d),\n\t\t")

	line_count = 0
	row_count = 0

	daterowcheck = int(DATETIMEFIELDNUMBER)-1

	#creating list to potentially hold more than one date
	datetimelist = []
	datetimelist.append(DATE_AND_TIME_FIELD)

	for line in open(SCHEMAFILENAME):
		line_count +=1

	for row in open(SCHEMAFILENAME):
		row_count +=1

		fields = row.split(",")

		#setting the variable to put the field number in the field output
		new_count = row_count-1

		if int(DATETIMEFIELDNUMBER) == line_count: #if the date and time column is the last column within the data, then...
			if row_count == daterowcheck: #if the row number equals the row before the last row, that holds the date and time column then print that line with no comma at the end.
				if fields[0] not in datetimelist:
					LOADFILEOUTPUT.write("\t\""+fields[0]+"\" : ""fields["+str(new_count)+"]\n \t\t\t")
				else:
					continue
			elif row_count != daterowcheck:
				if fields[0] not in datetimelist:
					LOADFILEOUTPUT.write("\t\""+fields[0]+"\" : ""fields["+str(new_count)+"],\n\t\t")
				else:
					continue
			else:
				continue
		elif int(DATETIMEFIELDNUMBER) != line_count:
			if row_count != line_count:
				if fields[0] not in datetimelist:
					LOADFILEOUTPUT.write("\t\""+fields[0]+"\" : ""fields["+str(new_count)+"],\n\t\t")
				else:
					continue
			elif row_count == line_count:
				if fields[0] not in datetimelist:
					LOADFILEOUTPUT.write("\t\""+fields[0]+"\" : ""fields["+str(new_count)+"]\n \t\t\t")
				else:
					continue
		else:
			"There was something wrong with the data that you tried to use. Please check the data and try again"


	LOADFILEOUTPUT.write("})\n\t\ttry:\n \t\t\t")
	LOADFILEOUTPUT.write("conn.bulk_index(PROJECTNAME,INDEXNAME,data)\n\t\t\tcorrect_counter += 1 \n\t\t")
	LOADFILEOUTPUT.write("except Exception as e: \n \t\t\t")
	LOADFILEOUTPUT.write("for i in data:\n\t\t\t\tfailurelist.append(data)\n \t\t\tfailed_count += 1\n \t\t")
	LOADFILEOUTPUT.write("data = []\n\n")

	LOADFILEOUTPUT.write("if correct_counter!=0:\n\tprint \"\\n\",correct_counter,\" rows were successfully loaded into ES \\n\"\nelif correct_counter==0:\n\tprint \"\\n\",correct_counter,\" rows were loaded into elasticsearch.\\n\" \n")

	LOADFILEOUTPUT.write(
	"\nif (failed_count != 0) & (correct_counter != 0):\n\tprint failed_count,\"rows failed to load - check error output file to see the specific data\"\nelif (failed_count == 0) & (correct_counter != 0):\n\tprint \"No rows failed to load\"\nelif (failed_count != 0) & (correct_counter == 0):\n\tprint \"No rows were succesfully loaded to elasticsearch, check data\"\nelif (failed_count == 0) & (correct_counter == 0):\n\tprint \"Your load data was not processed, check to make sure the load data you attempted to load matches the map index that you sent to es\"\nelse:\n\tprint \"Check data\"\n\nfor items in failurelist:\n \tERRORFILEOUTPUT.write(''+str(items)+\"\\n\")\n\nERRORFILEOUTPUT.close()")

	#Final close out for output file
	LOADFILEOUTPUT.close()

	print "\nLoadfile \""+NAMEOFLOADFILE+"\" created"

elif DATETIMEINPUT == "no":

	#creating output files to write data to
	LOADFILEOUTPUT = open(NAMEOFLOADFILE, "w")
	#Creating the firs section of the load file.
	LOADFILEOUTPUT.write("from pyelasticsearch import ElasticSearch \n")
	LOADFILEOUTPUT.write("import time, codecs \n\n")
	LOADFILEOUTPUT.write("correct_counter = 0"+"\n"+"failed_count = 0"+"\n"+"data = []"+"\nfailurelist = []\n\n")
	LOADFILEOUTPUT.write("conn = ElasticSearch('http://127.0.0.1:9200') \nstart = time.clock() \n\n")
	LOADFILEOUTPUT.write("input1 = raw_input ('Enter load data file name: ') \ninput2 = raw_input ('Enter Project Name: ') \ninput3 = raw_input ('Enter index name: ') \ninput4 = raw_input ('How many columns does your data have? ') \ninput5 = raw_input ('What is the delimiter? ') \ninput6 = raw_input ('Enter file name for error output (.txt) ') \ninput7 = raw_input ('Enter the codec that the data is formatted in: e.g. \\'ascii''') \n\nFILENAME = codecs.open(input1, 'r', str(input7)) \nPROJECTNAME = str(input2) \nINDEXNAME = str(input3) \nNUM_OF_FIELDS = int(input4) \nDELIMITER = str(input5) \nERRORFILEOUTPUT = open(input6, \"w\")\n\nprint \"Running...\"\n\n")
	LOADFILEOUTPUT.write("for line in FILENAME:\n\t")
	LOADFILEOUTPUT.write("fields = line.split(DELIMITER) \n\tif len(fields) == NUM_OF_FIELDS:\n\t\t")
	LOADFILEOUTPUT.write("data.append({\n\t\t")

	line_count = 0
	row_count = 0


	for line in open(SCHEMAFILENAME):
		line_count += 1

	for row in open(SCHEMAFILENAME):
		row_count +=1

		fields = row.split(",")
		new_count = row_count-1

		if row_count != line_count:
			LOADFILEOUTPUT.write("\t\""+fields[0]+"\" : ""fields["+str(new_count)+"],\n\t\t")
		elif row_count == line_count:
			LOADFILEOUTPUT.write("\t\""+fields[0]+"\" : ""fields["+str(new_count)+"]\n \t\t\t}) \n")
		else:
			"There was something wrong with the data that you tried to use. Please check the data and try again....wamp wamp"

	LOADFILEOUTPUT.write("\t\ttry:\n \t\t\t")
	LOADFILEOUTPUT.write("conn.bulk_index(PROJECTNAME,INDEXNAME,data)\n\t\t\tcorrect_counter += 1 \n\t\t")
	LOADFILEOUTPUT.write("except Exception as e: \n \t\t\t")
	LOADFILEOUTPUT.write("for i in data:\n\t\t\t\tfailurelist.append(data)\n \t\t\tfailed_count += 1\n \t\t")
	LOADFILEOUTPUT.write("data = []\n\n")

	LOADFILEOUTPUT.write("if correct_counter!=0:\n\tprint \"\\n\",correct_counter,\" rows were successfully loaded into ES \\n\"\nelif correct_counter==0:\n\tprint \"\\n\",correct_counter,\" rows were loaded into elasticsearch.\\n\" \n")

	LOADFILEOUTPUT.write(
	"\nif (failed_count != 0) & (correct_counter != 0):\n\tprint failed_count,\"rows failed to load - check error output file to see the specific data\"\nelif (failed_count == 0) & (correct_counter != 0):\n\tprint \"No rows failed to load\"\nelif (failed_count != 0) & (correct_counter == 0):\n\tprint \"No rows were succesfully loaded to elasticsearch, check data\"\nelif (failed_count == 0) & (correct_counter == 0):\n\tprint \"Your load data was not processed, check to make sure the load data you attempted to load matches the map index that you sent to es\"\nelse:\n\tprint \"Check data\" \n\nfor items in failurelist:\n \tERRORFILEOUTPUT.write(''+str(items)+\"\\n\")\n\nERRORFILEOUTPUT.close()")

	#Final close out for output file
	LOADFILEOUTPUT.close()
	print "\nLoadfile \""+NAMEOFLOADFILE+"\" created"

else:
	print "\nIncorrect input for date and time present request"