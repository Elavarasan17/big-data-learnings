#import statements
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types
import sys

#additional methods
def getKeyValuePairs(adjList):
    destList = adjList[1].strip().split(" ")
    return (int(adjList[0]), [int(i) for i in destList if len(i) > 0])

def producePaths(rddInput):
    source = rddInput[0]
    for x in rddInput[1][0]:
        distance = rddInput[1][1][1]
        yield (x, [source, distance + 1])

def shortDistance(pathRdd1, pathRdd2):
    return pathRdd1 if pathRdd1[1] < pathRdd2[1] else pathRdd2

#main method	
def main(input, output, src, dest):
    #read the nodes and graph edges
    graphEdges = sc.textFile(input+"/links-simple-sorted.txt").map(lambda x : x.split(":")).map(getKeyValuePairs).cache()
    destNode = int(dest)
    srcNode = int(src)
    route = sc.parallelize([(srcNode, [0, 0])]).cache()
    destReachFlag = False

    if srcNode == destNode:
        sentence = "You are already at destination because source is equal to destination"
        result = sc.parallelize(sentence.split(" "))
        result.saveAsTextFile(output + '/path')
    else:
        #Algorithm
        for i in range(6):
            newPathRdd = graphEdges.join(route.filter(lambda a: a[1][1] == i)).flatMap(producePaths)
            route = route.union(newPathRdd).reduceByKey(shortDistance)
            route.saveAsTextFile(output + '/iter-' + str(i))
            #break the loop if destination reached
            if not route.filter(lambda b: b[0] == destNode).isEmpty():
                destReachFlag = True
                break
        
        #finding the actual path
        if destReachFlag :
            node = destNode
            actualPath = []
            while(node != 0) :
                actualPath.append(node)
                node = route.filter(lambda x: x[0] == node).collect()[0][1][0]
            result = sc.parallelize(reversed(actualPath))

        else:
            sentence = "Destination cannot be reached because path was not found"
            result = sc.parallelize(sentence.split(" "))

        #writing to output
        result.saveAsTextFile(output + '/path')

#call manin method
if __name__ == '__main__':
    inputDirectory = sys.argv[1]
    source = sys.argv[3]
    destination = sys.argv[4]
    output = sys.argv[2]

    spark = SparkSession.builder.appName('Shortest Path').getOrCreate()
    # assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    main(inputDirectory, output, source, destination)