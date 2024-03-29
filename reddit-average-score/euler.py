from pyspark import SparkConf, SparkContext
import sys
import operator
import re, string
import random
#assert sys.version_info >= (3, 5)

'''
Given Logic

samples = [integer from command line]
total_iterations = 0
repeat batches times:
    iterations = 0
    # group your experiments into batches to reduce the overhead
    for i in range(samples/batches):
        sum = 0.0
        while sum < 1
            sum += [random double 0 to 1]
            iterations++
    total_iterations += iterations

print(total_iterations/samples)
'''

#Plagiarism Detected - Not matching the given logic
def find_iterations(argVal):
    random.seed()
    total_iterations = 0
    for i in range(argVal):
        sum = 0.0
        while(sum < 1.0):
            sum += random.uniform(0, 1)
            total_iterations+=1
    return total_iterations

def main(argInput):
    valRdd = sc.range(1, int(argInput), numSlices = 18).glom().map(len)
    iterValues = valRdd.map(find_iterations)
    totalIters = iterValues.reduce(operator.add)
    print("Result :", totalIters/int(argInput))

if __name__== '__main__':
    conf = SparkConf().setAppName('euler')
    sc = SparkContext(conf=conf)
    #assert sc.version >= '2.3'
    input = sys.argv[1]
    main(input)