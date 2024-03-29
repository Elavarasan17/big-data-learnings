#import statements
from pyspark import SparkConf, SparkContext
import json
import sys
import operator

#methods
def transformData(jsonInput):
    redditValue = jsonInput["subreddit"]
    scoreValue = jsonInput["score"]
    authorValue = jsonInput["author"]
    return (redditValue, scoreValue, authorValue)

def filterData(inputArgs):
	return True if "e" in inputArgs[0] else False

def filterPostives(inputArgs):
        return True if inputArgs[1] > 0 else False
   

def filterNegatives(inputArgs):
        return True if inputArgs[1] < 0 else False

#main method	
def main(inputs, output):
    text = sc.textFile(inputs)
    inputJson = text.map(json.loads).map(transformData).filter(filterData).cache()
    inputJson.filter(filterPostives).map(json.dumps).saveAsTextFile(output + '/positive')
    inputJson.filter(filterNegatives).map(json.dumps).saveAsTextFile(output + '/negative')

if __name__ == '__main__':
    conf = SparkConf().setAppName('Reddit ETL')
    sc = SparkContext(conf=conf)
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)