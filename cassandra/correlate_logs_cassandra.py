#import statements
import sys
from pyspark.sql import SparkSession, functions
import math

#main method	
def main(keyspace, table):
    #Reading from Cassandra Table
    df = spark.read.format("org.apache.spark.sql.cassandra").options(table=table, keyspace=keyspace).load()

    logsDF = df.select(["hostname", "bytes"])

    tempDF = logsDF.groupBy("hostname").count()
    changeData = logsDF.groupBy('hostname').agg(functions.sum(logsDF.bytes).alias('BytesSum'))

    joinCondition1 = [changeData.hostname == tempDF.hostname]
    datapoint = tempDF.alias('a').join(changeData.alias('b'), joinCondition1).select(['a.hostname','count','BytesSum'])
    datapoint = datapoint.withColumn('ones', functions.lit(1)).withColumn('BytesSum2', datapoint.BytesSum ** 2).withColumn('count2', datapoint['count'] ** 2).withColumn('XY', datapoint['count']*datapoint['BytesSum'])

    #sum all the columns
    summedDf = datapoint.groupBy().sum().collect()[0]
    numDtPts, ReqNumSum, BytesNumSum, Req2Sum, Bytes2sum, xy = summedDf['sum(ones)'], summedDf['sum(count)'], summedDf['sum(BytesSum)'], summedDf['sum(count2)'], summedDf['sum(BytesSum2)'], summedDf['sum(XY)']
    rvalue = (numDtPts*xy - (ReqNumSum * BytesNumSum))/(math.sqrt(numDtPts* Req2Sum - math.pow(ReqNumSum,2)) * math.sqrt(numDtPts* Bytes2sum - math.pow(BytesNumSum,2)))
    r2value = math.pow(rvalue,2)

    #output the values
    print("r = ", rvalue)
    print("r^2 = ", r2value)

#calling main method
if __name__ == '__main__':
    #arguments
    keyspace = sys.argv[1]
    tableName = sys.argv[2]

    #spark-cassandra connector
    cluster_seeds = ['node1.local', 'node2.local']
    spark = SparkSession.builder.appName('Load Logs Spark-Cassandra').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext   

    #initial connection
    main(keyspace, tableName)