from pyspark import SparkConf, SparkContext
import json
import sys
import operator
#assert sys.version_info >= (3, 5)

#necessary methods
def getKeyValuePairs(inputComment):
	redditKey = inputComment["subreddit"]
	score = inputComment["score"]
	return redditKey, (1, score)
	
def sumPairs(pair1, pair2):
    count = pair1[0] + pair2[0]
    score = pair1[1] + pair2[1]
    return count, score

def getAverage(argPair):
    average = argPair[1][1] / argPair[1][0]
    if average > 0:
        return argPair[0], average

def returnAuthorScores(inputArgs):
    relativeScore = inputArgs[1][0]["score"] / inputArgs[1][1]
    authorName = inputArgs[1][0]["author"]
    return relativeScore, authorName

def getKey(arg):
    return arg[0]	

#main method	
def main(inputs, output):
    text = sc.textFile(inputs) 
    commentData = text.map(json.loads).cache() 
    averages = commentData.map(getKeyValuePairs).reduceByKey(sumPairs).map(getAverage)
    commentbysub = commentData.map(lambda c: (c['subreddit'], c)).join(averages)
    outdata = commentbysub.map(returnAuthorScores).sortBy(getKey,ascending=False)
    outdata.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('Reddit Averages')
    sc = SparkContext(conf=conf)
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)