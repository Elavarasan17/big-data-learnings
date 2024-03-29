#import statements
import sys
from pyspark.sql import SparkSession, functions, types
from pyspark.ml.feature import VectorAssembler, SQLTransformer
from pyspark.ml import Pipeline
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

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
def main(input, output):
    weather_data = spark.read.csv(input, schema=tmax_schema)

    #sql query
    transform_query = 'SELECT today.station, dayofyear(today.date) as dayofyear, today.latitude, today.longitude, today.elevation, today.tmax, yesterday.tmax AS yesterday_tmax FROM __THIS__ as today INNER JOIN __THIS__ as yesterday ON date_sub(today.date, 1) = yesterday.date AND today.station = yesterday.station'
    # transform_query = 'SELECT today.station, dayofyear(today.date) as dayofyear, today.latitude, today.longitude, today.elevation, today.tmax FROM __THIS__ as today'

    #split the dataset
    train, validation = weather_data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()

    #transform data
    sqlTrans = SQLTransformer(statement=transform_query)

    #pipeline
    # feature_assembler = VectorAssembler(inputCols=['latitude', 'longitude', 'elevation', 'dayofyear'], outputCol="features")
    feature_assembler = VectorAssembler(inputCols=['latitude', 'longitude', 'elevation', 'yesterday_tmax', 'dayofyear'], outputCol="features")
    regression_model = GBTRegressor(featuresCol="features", labelCol="tmax", maxDepth=6, predictionCol="prediction")
    weather_pipeline = Pipeline(stages=[sqlTrans, feature_assembler, regression_model])
    weather_model = weather_pipeline.fit(train)
    
    #prediction
    predict_values = weather_model.transform(validation)
    
    #evaluator
    r2_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax', metricName='r2')  
    print("r2 :", r2_evaluator.evaluate(predict_values))

    rmse_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax', metricName='rmse')  
    print("rmse :", rmse_evaluator.evaluate(predict_values))

    #save the model
    weather_model.write().overwrite().save(output)

if __name__ == "__main__":
    path =  sys.argv[1]
    outputdir = sys.argv[2]
    spark = SparkSession.builder.appName('Weather Train').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    # assert spark.version >= '3.0' # make sure we have Spark 3.0+
    main(path, outputdir)