#import statements
#Elavarasan Murthy
import sys
from pyspark.sql import SparkSession, functions, types

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
    weatherData.createOrReplaceTempView("weatherData")
    
    #filteredQFlagValues = weatherData.filter(weatherData.qFlag.isNull())
    filterSqlFlag = spark.sql("SELECT * FROM WEATHERDATA WHERE QFLAG IS NULL")
    filterSqlFlag.createOrReplaceTempView("TQFILTERDATA")

    #tMaxData = filteredQFlagValues.filter(filteredQFlagValues.Observation.like('TMAX')).withColumn('maxValue', filteredQFlagValues.Value/10.0).select(['Date', 'Station','maxValue'])
    tMaxData = spark.sql("SELECT TQFILTERDATA.DATE, STATION, VALUE/10.0 AS MAXVALUE FROM TQFILTERDATA WHERE OBSERVATION LIKE 'TMAX'")  
    tMaxData.createOrReplaceTempView("TMAXDATA")

    # tMinData = filteredQFlagValues.filter(filteredQFlagValues.Observation.like('TMIN')).withColumn('minValue', filteredQFlagValues.Value/10.0).select(['Date', 'Station','minValue'])
    tMinData = spark.sql("SELECT TQFILTERDATA.DATE, STATION, VALUE/10.0 AS MINVALUE FROM TQFILTERDATA WHERE OBSERVATION LIKE 'TMIN'")  
    tMinData.createOrReplaceTempView("TMINDATA")

    # joinCondition1 = [tMaxData.Date == tMinData.Date, tMaxData.Station == tMinData.Station]
    # minMaxData = tMinData.alias('minDF').join(tMaxData.alias('maxDF'), joinCondition1).select(['minDF.Date', 'maxDF.Station', 'maxDF.maxValue', 'minDF.minValue'])
    minMaxData = spark.sql("SELECT MID.DATE, MXD.STATION, MXD.MAXVALUE, MID.MINVALUE FROM TMAXDATA AS MXD JOIN TMINDATA AS MID ON MXD.DATE = MID.DATE AND MXD.STATION = MID.STATION")
    minMaxData.createOrReplaceTempView("TMINMAXDATA")

    # rangeData = minMaxData.withColumn('Range', functions.round(minMaxData.maxValue-minMaxData.minValue,1)).select(['Date', 'Station', 'Range'])
    # maxRangeData = rangeData.groupBy('Date').agg(functions.max(rangeData.Range).alias('maxRange'))
    rangeData = spark.sql("SELECT TMINMAXDATA.DATE, STATION, ROUND((MAXVALUE-MINVALUE), 1) AS RANGE FROM TMINMAXDATA")
    rangeData.createOrReplaceTempView("TRANGEDATA")
    maxRangeData = spark.sql("SELECT TRANGEDATA.DATE, MAX(RANGE) AS MAXRANGE FROM TRANGEDATA GROUP BY TRANGEDATA.DATE")
    maxRangeData.createOrReplaceTempView("TMAXRANGEDATA")

    # joinCondition2 = [rangeData.Date == maxRangeData.Date, maxRangeData.maxRange == rangeData.Range]
    # tempDiff = rangeData.alias('a1').join(maxRangeData.alias('b1'), joinCondition2).select(['a1.Date', 'a1.Station', 'b1.maxRange'])
    tempDiff = spark.sql("SELECT TRD.DATE, TRD.STATION, CAST(TMD.MAXRANGE AS DECIMAL(10,2))  FROM TRANGEDATA AS TRD JOIN TMAXRANGEDATA AS TMD ON TRD.DATE = TMD.DATE AND TMD.MAXRANGE = TRD.RANGE")
    tempDiff.createOrReplaceTempView("TEMPDIFFDATA")

    #tempDiff.sort([tempDiff.Date, tempDiff.Station]).show()
    result = spark.sql("SELECT DATE, STATION, MAXRANGE FROM TEMPDIFFDATA ORDER BY DATE ASC")
    
    result.show()
    result.write.csv(output + "/result")

#calling main method
if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('Weather Dataframe code').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)