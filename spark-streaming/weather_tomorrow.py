#import statements
import sys
from pyspark.sql import SparkSession, types
from pyspark.ml import PipelineModel
from datetime import datetime

#schema definition
tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])

#main method
def main(inputmodel):
    test_dataframe = spark.createDataFrame([('sfubigdatalab',datetime.strptime('19/11/2021', '%d/%m/%Y').date() ,49.2771, 122.9146, 330.0, 12.0),('sfubigdatalab', datetime.strptime('20/11/2021', '%d/%m/%Y').date() ,49.2771, 122.9146, 330.0, 0.0)], schema=tmax_schema)
    
    # load the model
    model = PipelineModel.load(inputmodel)
    
    # use the model to make predictions
    prediction = model.transform(test_dataframe)

    # If you used a regressor that gives .featureImportances, maybe have a look...
    # print(model.stages[-1].featureImportances)
    
    print("Predicted tmax tomorrow :", prediction.take(1)[0][-1])

if __name__ == "__main__":
    path =  sys.argv[1]
    spark = SparkSession.builder.appName('Weather Tomorrow').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    # assert spark.version >= '3.0' # make sure we have Spark 3.0+
    main(path)