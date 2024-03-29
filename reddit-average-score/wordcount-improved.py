from pyspark import SparkConf, SparkContext
import sys
import operator
import re, string

def words_once(line):
    wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation)) #not efficient way of using it because the function is called multiple times.
    for w in wordsep.split(line):
        if(len(w)>0):
            yield(w.lower(), 1)
			
def get_key(kv):
	return kv[0]
	
def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)


def main(inputs, output):
    text = sc.textFile(inputs).repartition(8)
    words = text.flatMap(words_once)
    wordcount = words.reduceByKey(operator.add)
    outdata = wordcount.sortBy(get_key).map(output_format)
    outdata.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('wordcount improved')
    sc = SparkContext(conf=conf)
   #assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)