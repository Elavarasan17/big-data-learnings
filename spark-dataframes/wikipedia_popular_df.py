#import Statements
#Elavarasan Murthy
import sys
from pyspark.sql import SparkSession, functions, types

#Schema Definition
wikipedia_schema = types.StructType([
    types.StructField('Language', types.StringType()),
    types.StructField('Title', types.StringType()),
    types.StructField('Count', types.IntegerType()),
    types.StructField('Size', types.LongType())
])

#additional methods
@functions.udf(returnType=types.StringType())
def path_to_hour(path):
    stamp = path.split("/")[-1]
    timeArray = stamp.split("-")
    return timeArray[1] +"-"+ timeArray[2][0:2]

#main method
def main(inputs):
    pages = spark.read.csv(inputs, schema=wikipedia_schema, sep=" ").withColumn('filename', functions.input_file_name())
    
    #converting String filename to hour
    hourPages = pages.withColumn('hour', path_to_hour(pages['filename'])).drop('filename')
    
    #filtering wikipedia corpus
    filterTitlePages = hourPages.Title.like('Main_Page') & hourPages.Title.startswith('Special: ')
    filteredPages = hourPages.filter(hourPages.Language.like('en') & ~filterTitlePages).cache()

    #finding the max views
    freqPages = filteredPages.groupBy('hour').agg(functions.max(filteredPages.Count).alias('viewsMax'))
    joinCondition = [hourPages.hour == freqPages.hour, hourPages.Count == freqPages.viewsMax]

    #join filteredWikipedia and Frequently visited pages
    result = filteredPages.join(functions.broadcast(freqPages), joinCondition).select(freqPages.hour, filteredPages.Title, freqPages.viewsMax).sort(freqPages.hour)
    result.show()

#calling main function
if __name__ == '__main__':
    inputs = sys.argv[1]
    spark = SparkSession.builder.appName('Wikipedia Data Frames').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs)