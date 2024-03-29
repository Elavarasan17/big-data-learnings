from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types
import math
import sys
import re

#regex compiler
line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')

#Schema Definition
logs_schema = types.StructType([
    types.StructField('Hostname', types.StringType()),
    types.StructField('Datetime', types.StringType()),
    types.StructField('RequestedPath', types.StringType()),
    types.StructField('Bytes', types.StringType())
])

#methods
def getCorrectLogs(logLine):
    return line_re.split(logLine)[1:-1]

#main method	
def main(inputs):
    #read input file
    logsRDD = sc.textFile(inputs).map(getCorrectLogs).filter(lambda x: len(x) == 4)

    logsDF = spark.createDataFrame(logsRDD, schema = logs_schema)["Hostname", "Bytes"]
    logsDF = logsDF.withColumn("Bytes", logsDF.Bytes.cast(types.IntegerType()))

    #Extracting required data
    tempDF = logsDF.groupBy("Hostname").count()
    changeData = logsDF.groupBy('Hostname').agg(functions.sum(logsDF.Bytes).alias('BytesSum'))

    joinCondition1 = [changeData.Hostname == tempDF.Hostname]
    datapoint = tempDF.alias('a').join(changeData.alias('b'), joinCondition1).select(['a.Hostname','count','BytesSum'])
    datapoint = datapoint.withColumn('ones', functions.lit(1)).withColumn('BytesSum2', datapoint.BytesSum ** 2).withColumn('count2', datapoint['count'] ** 2).withColumn('XY', datapoint['count']*datapoint['BytesSum'])

    #sum all the columns
    summedDf = datapoint.groupBy().sum().collect()[0]
    numDtPts, ReqNumSum, BytesNumSum, Req2Sum, Bytes2sum, xy = summedDf['sum(ones)'], summedDf['sum(count)'], summedDf['sum(BytesSum)'], summedDf['sum(count2)'], summedDf['sum(BytesSum2)'], summedDf['sum(XY)']
    rvalue = (numDtPts*xy - (ReqNumSum * BytesNumSum))/(math.sqrt(numDtPts* Req2Sum - math.pow(ReqNumSum,2)) * math.sqrt(numDtPts* Bytes2sum - math.pow(BytesNumSum,2)))
    r2value = math.pow(rvalue,2)

    #output the values
    print("r = ", rvalue)
    print("r^2 = ", r2value)

if __name__ == '__main__':
    inputs = sys.argv[1]
    spark = SparkSession.builder.appName('Correlate Logs').getOrCreate()
    # assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs)