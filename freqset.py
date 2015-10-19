# -*- coding: utf-8 -*- 
import MapReduce
import sys
import itertools

mr = MapReduce.MapReduce()


def mapper(record):
    #Initialising an empty list for the combinations 
    combi = []
    for n in record:
      n = int(n)

    #Generating combinations of two’s
    combi = list(itertools.combinations(record,2))

    #Emitting each of the combinations and 1
    for i in combi:
    	mr.emit_intermediate(i, 1)
      

def reducer(key, list_of_values):
    # key: Each combination
    # value: list of occurrence counts
   
    total = 0
    #calculating the count for each combination
    for v in list_of_values:
      total += v

    #Checking if the itemset count is greater than the given support 100
    if total > 100:
      mr.emit((key))

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
