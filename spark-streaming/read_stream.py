#import statements
import sys
from pyspark.sql import SparkSession, functions, types

#main method
def main(topic):
    messages = spark.readStream.format('kafka').option('kafka.bootstrap.servers', 'node1.local:9092,node2.local:9092').option('subscribe', topic).load()
    values = messages.select(messages['value'].cast('string'))
    # values.show()

    xy_df = functions.split(values['value'], " ")
    plane_points = values.withColumn('x', xy_df.getItem(0).cast(types.FloatType())).withColumn('y', xy_df.getItem(1).cast(types.FloatType()))
    plane_points = plane_points.select('x', 'y').withColumn('xy', plane_points['x']*plane_points['y']).withColumn('x2', plane_points['x']**2).withColumn('n', functions.lit(1))
    
    lin_reg = plane_points.agg(functions.sum('x'), functions.sum('y'), functions.sum('xy'), functions.sum('x2'), functions.sum('n'))
    lin_reg = lin_reg.withColumn('slope', (lin_reg['sum(xy)'] - (1/lin_reg['sum(n)'])*lin_reg['sum(x)']*lin_reg['sum(y)'])/(lin_reg['sum(x2)'] - (1/lin_reg['sum(n)'])*(lin_reg['sum(x)']**2)))
    lin_reg = lin_reg.withColumn('Intercept', ((lin_reg['sum(y)']/lin_reg['sum(n)']) - (lin_reg['slope'] * (lin_reg['sum(x)']/lin_reg['sum(n)']))))
    lin_reg = lin_reg.select(['slope', 'Intercept'])

    stream = lin_reg.writeStream.format('console').outputMode('complete').start()
    stream.awaitTermination(120)

if __name__ == '__main__':
    topic = sys.argv[1]
    spark = SparkSession.builder.appName('Read Stream').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    # assert spark.version >= '3.0' # make sure we have Spark 3.0+
    main(topic)