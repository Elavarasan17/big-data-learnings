#import statements
import sys
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from colour_tools import colour_schema, rgb2lab_query, plot_predictions
from pyspark.sql import SparkSession

#main method
def main(inputs):
    #read input
    data = spark.read.csv(inputs, schema=colour_schema)

    #split the dataset
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()
    
    #pipeline process
    rgb_assembler = VectorAssembler(inputCols=["R", "G", "B"], outputCol="features")
    word_indexer = StringIndexer(inputCol="word", outputCol="indexed", handleInvalid="error", stringOrderType="frequencyDesc")
    classifier = MultilayerPerceptronClassifier(labelCol="indexed",layers=[3, 30, 11])

    # create a pipeline to predict RGB colours -> word
    rgb_pipeline = Pipeline(stages=[rgb_assembler, word_indexer, classifier])
    rgb_model = rgb_pipeline.fit(train)
    
    #predict the values
    predict_values = rgb_model.transform(validation)
    
    # create an evaluator and score the validation data
    evaluator = MulticlassClassificationEvaluator(labelCol='indexed',predictionCol="prediction")
    score = evaluator.evaluate(predict_values)
    plot_predictions(rgb_model, 'RGB', labelCol='word')
    print('Validation score for RGB model: %g' % (score, ))
    #-------------------------------------------------------------------------------------------------#
    
    #Feature Engineering
    rgb_to_lab_query = rgb2lab_query(passthrough_columns=['word'])
    sqlTrans = SQLTransformer(statement=rgb_to_lab_query)
    lab_assembler = VectorAssembler(inputCols=["labL", "labA", "labB"], outputCol="features")
    lab_word_indexer = StringIndexer(inputCol="word", outputCol="indexed", handleInvalid="error", stringOrderType="frequencyDesc")
    lab_classifier = MultilayerPerceptronClassifier(labelCol="indexed",layers=[3, 30, 11])
    
    # create a pipeline RGB colours -> LAB colours -> word; train and evaluate.
    labcolor_pipeline = Pipeline(stages=[sqlTrans, lab_assembler, lab_word_indexer, lab_classifier])
    lab_model = labcolor_pipeline.fit(train)
    
    #predict the values
    predict_values = lab_model.transform(validation)

    # create an evaluator and score the validation data
    lab_evaluator = MulticlassClassificationEvaluator(labelCol='indexed',predictionCol="prediction")
    plot_predictions(lab_model, 'LAB', labelCol='word')
    lab_score = lab_evaluator.evaluate(predict_values)
    print('Validation score for Lab color model: %g' % (lab_score, ))
    

if __name__ == '__main__':
    path = sys.argv[1]
    #assert spark.version >= '2.4' # make sure we have Spark 2.4+
    spark = SparkSession.builder.appName('colour prediction').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    main(path)
