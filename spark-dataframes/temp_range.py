#import functions
#Elavarasan Murthy
import sys
from pyspark.sql import SpakSession, functions, types

#Schema Definition
weather_schema = types.StructType([
    types.StructField('Station', types.StringType()),
    types.StructField('Date', types.StringType()),
    types.StructField('Observation', types.StringType()),
    types.StructField('Value', types.IntegerType()),
    types.StructField('mFlag', types.StringType()),
    types.StructField('qFlag', types.StringType()),
    types.StructField('sFlag', types.StringType()),
    types.StructField('obsTime', types.StringType()),
])

def main(inputs, output):
    # main logic starts here
    weatherData = spark.read.csv(inputs, schema=weather_schema, sep=",")
    
    #filtering the Data
		filteredQFlagValues = weatherData.filter(weatherData.qFlag.isNull())
    tMaxData = filteredQFlagValues.filter(filteredQFlagValues.Observation.like('TMAX')).withColumn('maxValue', filteredQFlagValues.Value/10.0).select(['Date', 'Station','maxValue'])
    tMinData = filteredQFlagValues.filter(filteredQFlagValues.Observation.like('TMIN')).withColumn('minValue', filteredQFlagValues.Value/10.0).select(['Date', 'Station','minValue'])

    #Combining min and max values to one data frame
    joinCondition1 = [tMaxData.Date == tMinData.Date, tMaxData.Station == tMinData.Station]
    minMaxData = tMinData.alias('minDF').join(tMaxData.alias('maxDF'), joinCondition1).select(['minDF.Date', 'maxDF.Station', 'maxDF.maxValue', 'minDF.minValue'])

    #finding the maximum range for each date
    rangeData = minMaxData.withColumn('Range', functions.round(minMaxData.maxValue-minMaxData.minValue,1)).select(['Date', 'Station', 'Range'])
    maxRangeData = rangeData.groupBy('Date').agg(functions.max(rangeData.Range).alias('maxRange'))

    #Finding the station with maximum range on the given date
    joinCondition2 = [rangeData.Date == maxRangeData.Date, maxRangeData.maxRange == rangeData.Range]
    tempDiff = rangeData.alias('a1').join(maxRangeData.alias('b1'), joinCondition2).select(['a1.Date', 'a1.Station', 'b1.maxRange'])

    tempDiff.sort([tempDiff.Date, tempDiff.Station]).show()
    tempDiff.write.csv(output + "/result")

#calling main method
if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('Weather Dataframe code').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)