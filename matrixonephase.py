# -*- coding: utf-8 -*- 
import MapReduce
import sys
import re
import string
from collections import Counter

mr = MapReduce.MapReduce()

def mapper(record):
    # key: document identifier
    # value: document contents
    doc = str(record[0])
    value =str(record[1])
    words = value.split()

    #Matching each word with a regular expression to check for only alpha numeric characters
    for w in words:
	match = re.match(r'^[A-Za-z0-9 ]*$', w )
	if match:
		#converting all the words into lower string
      		w=w.lower()
    		mr.emit_intermediate(w,doc)
   

def reducer(key, list_of_values):
    # key: word
    # value: List of documents in which the word is present
    tf=0
    df=0
    tf1=[]

    #counting the occurrences of each document in the list 
    tf=Counter(list_of_values)

    #converting the dictionary of document:value into a list(document,value) to find term frequency in the form of a list
    tf1=[(k,v) for k,v in tf.items()]  

    # Finding the Document frequency
    df=len(tf)
    mr.emit((key,df,tf1))

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
