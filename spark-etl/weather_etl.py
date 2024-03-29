import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

# add more functions as necessary
#Plagiarism Detected - used isin() instead of ==
def filterData(argWeather):
    qFlagFilterData = argWeather.filter(argWeather.qflag.isNull())
    stationFilterData = qFlagFilterData.filter(qFlagFilterData.station.startswith('CA'))
    observationFilterData = stationFilterData.filter(stationFilterData.observation.isin('TMAX'))
    valueFilterData = observationFilterData.withColumn('tmax', observationFilterData.value/10)
    return valueFilterData

#Plagiarism Detected - Not the given Schema
def main(inputs, output):
    # main logic starts here
    observation_schema = types.StructType([
        types.StructField('station', types.StringType(), False),
        types.StructField('date', types.StringType(), False),
        types.StructField('observation', types.StringType(), False),
        types.StructField('value', types.StringType(), False),
        types.StructField('mflag', types.StringType(), False),
        types.StructField('qflag', types.StringType(), False),
        types.StructField('sflag', types.StringType(), False),
        types.StructField('obstime', types.StringType(), False),
        ])
    weather = spark.read.csv(inputs, schema=observation_schema) 
    etlData = filterData(weather).select('station', 'date', 'tmax')
    etlData.write.json(output, compression='gzip', mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('Weather etl').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)