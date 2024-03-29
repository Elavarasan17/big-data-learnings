from pyspark import SparkConf, SparkContext
import json
import sys
import operator
#assert sys.version_info >= (3, 5)

def getKeyValuePairs(jsonInput):
	redditKey = jsonInput["subreddit"]
	score = jsonInput["score"]
	return redditKey, (1, score)
	
def sumPairs(pair1, pair2):
    count = pair1[0] + pair2[0]
    score = pair1[1] + pair2[1]
    return count, score

def averg(argPair):
    average = argPair[1][1] / argPair[1][0]
    return argPair[0],average

def get_key(arg):
    return arg[0]	

#main method	
#Plagiarism Detected - .coalesce(1) is unnecessary
def main(inputs, output):
    text = sc.textFile(inputs) #read lines of json input values.
    redditPairs = text.map(json.loads).map(getKeyValuePairs) #map each json string to json objects and get key value pairs.
    outputPairs = redditPairs.reduceByKey(sumPairs).coalesce(1) #reduce by key and combine together.
    outdata = outputPairs.sortBy(get_key).map(averg).map(json.dumps)
    outdata.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('Reddit Averages')
    sc = SparkContext(conf=conf)
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)