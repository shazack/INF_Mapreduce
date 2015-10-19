# -*- coding: utf-8 -*- 
import MapReduce
import sys
import itertools
from collections import defaultdict
import json

mr = MapReduce.MapReduce()

def mapper(record):
        #Assigning the values of the sparse matrix to i,j,A[ij]
	i = record[0]
        j = record[1]
        aij = record[2]
	#Sorting is done by using the value j
        mr.emit_intermediate(j, ('A',i, aij))   
	mr.emit_intermediate(i, ('B',j, aij))
	
	
def reducer(key, values):
	a={}
	b={}
	mul=[]
	#Multiplication is done and the values are emitted in sparse matrix format
	for v1,v2,v3 in values:
	        if v1 =='A':
        	        a[v2]=v3
        	else:
                	b[v2]=v3
	
	for k1,val1 in a.iteritems():
		for k2,val2 in b.iteritems():
			mr.emit((k1,k2,val1*val2))
			

if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)

