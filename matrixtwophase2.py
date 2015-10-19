# -*- coding: utf-8 -*- 
import MapReduce
import sys
import itertools


mr = MapReduce.MapReduce()

def mapper(record):
	#The mapper just passes the values to the reducer
        i = record[0]
     	j=record[1]
        val=record[2]
	mr.emit_intermediate((i,j),val)
      
	
def reducer(key, values):
	#The sum is done corresponding to the each value in the matrix
	sum=0
	for i in values:
		sum+=i  
		
	mr.emit((key[0],key[1],sum))


if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
