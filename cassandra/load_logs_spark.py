#import statements
from pyspark import SparkContext
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession, types
from datetime import datetime
import uuid
import re
import sys

#regex compiler
line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')

#Schema Definition
logs_schema = types.StructType([
    types.StructField('id', types.StringType()),
    types.StructField('hostname', types.StringType()),
    types.StructField('datetime', types.TimestampType()),
    types.StructField('path', types.StringType()),
    types.StructField('bytes', types.IntegerType())
])

#methods
def getCorrectLogs(logLine):
    return line_re.split(logLine)[1:-1]

#['205.199.120.118', '01/Aug/1995:00:36:15', '/cgi-bin/imagemap/countdown70?59,186', '96']
def convertData(oneLogInput):
    id = str(uuid.uuid4())
    hostname = oneLogInput[0]
    datetimevalue = datetime.strptime(oneLogInput[1], '%d/%b/%Y:%H:%M:%S')
    path = oneLogInput[2]
    bytes = int(oneLogInput[3])
    newLogData = (id, hostname, datetimevalue, path, bytes)
    return newLogData

#main function
def main(input, outKeySpace, table):

    #read input file
    logsData = sc.textFile(input).map(getCorrectLogs).filter(lambda x: len(x) == 4)
    newLogsData = logsData.map(convertData)

    logsDataFrame = spark.createDataFrame(newLogsData, logs_schema).repartition(8)
    #writing to cassandra
    logsDataFrame.write.format("org.apache.spark.sql.cassandra").mode('Append').options(table=table, keyspace=outKeySpace).save()

#calling main method
if __name__ == '__main__':
    #arguments
    inputDir = sys.argv[1]
    keyspace = sys.argv[2]
    tableName = sys.argv[3]

    #spark-cassandra connector
    cluster_seeds = ['node1.local', 'node2.local']
    spark = SparkSession.builder.appName('Load Logs Spark-Cassandra').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext   

    #initial connection
    main(inputDir, keyspace, tableName)