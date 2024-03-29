from pyspark import SparkConf, SparkContext
import sys
import re, string

#Initialize Variables
wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))

inputs = sys.argv[1]
output = sys.argv[2]

#Spark Configuration
conf = SparkConf().setAppName('word count improved')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

#methods
def words_once(line):
    for w in wordsep.split(line):
        strippedWord = w.strip()
        yield (strippedWord.lower(), 1)

def add(x, y):
    return x + y

def get_key(kv):
    return kv[0]

def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)

text = sc.textFile(inputs)
words = text.flatMap(words_once)
filteredWords = words.filter(lambda wrd : len(wrd[0])>0)
wordcount = filteredWords.reduceByKey(add)

outdata = wordcount.sortBy(get_key).map(output_format)
outdata.saveAsTextFile(output)
