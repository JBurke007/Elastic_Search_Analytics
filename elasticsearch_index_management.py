#-------------------------------------------------------------------------------
# Name:        elasticsearch_index_management
# Purpose:      To create and manage indexes and sub indexes in elastic search
#
# Author:      jburke007
#
# Copyright:   (c) jburke007 2014
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import requests

print "\nSelect one of the options below:"
print "\n \t -create project \n \t -delete project \n \t -delete project index\n"


DECISION = raw_input ('Enter one of the options above: ')

if DECISION == "create project":
	print "\n....Project Name must be lowercase....\n"
	PROJECTNAME_INPUT = raw_input ('Enter new project name: ')
	PROJECTNAME = "http://localhost:9200/"+PROJECTNAME_INPUT+"/"

	r = requests.put(PROJECTNAME)

	if r.status_code == 200:
		print "Project created"
	elif r.status_code == 400:
		print "Project name contained capital letters or is already in use, please use another name"
	else:
		print "Project not created - make sure the project name is not already in use."
	#print r.text # used for reviewing the error code returned from elastic search
elif DECISION == "delete project":
	PROJECTNAME_INPUT = raw_input ('Enter project to delete: ')
	PROJECTNAME = "http://localhost:9200/"+PROJECTNAME_INPUT+"/"

	#requests.put(PROJECTNAME)
	r = requests.delete(PROJECTNAME)

	if r.status_code == 200:
		print "Project deleted"
	elif r.status_code == 404:
		print "Project does not exists, make sure you spelled the index correctly"
	else:
		print "Project was not deleted, check to see if the project already exists"

elif DECISION == "delete project index":
	PROJECTNAME_INPUT1 = raw_input ('Enter project name: ')
	PROJECTNAME_INPUT2 = raw_input ('Enter project index to delete: ')
	PROJECTNAME = "http://localhost:9200/"+PROJECTNAME_INPUT1+"/"+PROJECTNAME_INPUT2+"/"

	#requests.put(PROJECTNAME)
	r = requests.delete(PROJECTNAME)

	if r.status_code == 200:
		print "Project index deleted"
	elif r.status_code == 404:
		print "Project index does not exist, make sure you spelled the index correctly"
	else:
		print "Project index was not deleted, check to see if the index actually exists"

else:
	print "Incorrect input - try again"