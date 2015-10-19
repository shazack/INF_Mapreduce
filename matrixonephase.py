# -*- coding: utf-8 -*- 
import MapReduce
import sys
import itertools

mr = MapReduce.MapReduce()

def mapper(record):
        #Assigning the values of the sparse matrix to i,j and the value to A[ij]
	i = record[0]
        j = record[1]
        aij = record[2]
	
	#emitting values from each column in B for each value in A and vice versa for B 
        for k in range(5):
	 	 mr.emit_intermediate((i, k), ('A',j, aij))   
		 mr.emit_intermediate((k,j), ('B', i, aij))

	
def reducer(key, values):
   	a={}
	b={}
	sum=0
	#Checking the list 'values' to multiply the corresponding values of A and B when the values of j are equal and then sum
	for v1,v2,v3 in values:
	        if v1 =='A':
        	        a[v2]=v3
        	else:
                	b[v2]=v3
	#Here k1 and k2 are the keys representing j in A and B 
	for k1,val1 in a.iteritems():
		for k2,val2 in b.iteritems():
			if k1==k2:
				sum= sum + val1*val2 
	mr.emit((key[0],key[1],sum))


if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
