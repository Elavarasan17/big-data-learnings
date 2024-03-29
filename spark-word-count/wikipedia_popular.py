# Conditions 1.report only english wikipedia pages 2.Filter out the pages with the title "Main_Page" and "Special:"

#import statements
from pyspark import SparkConf, SparkContext
import sys
import re, string

#initialize variables
inputs = sys.argv[1]
output = sys.argv[2]

#Spark Configuration
conf = SparkConf().setAppName('Wikipedia Popular')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

#methods
#input example : "20160801-020000 en Aaaah 20 231818"
#output example: 20160801-000000 (146, 'Simon_Pegg')
def wikipedia_words(line):
	wikiTuples = tuple(line.split())
	yield(wikiTuples)

def tab_separated(kv):
	return "%s\t%s" % (kv[0], kv[1])

def get_key(kv):
	return kv[0]

def filter_pages(w):
	title = w[2]
	if(w[1]=="en" and title != "Main_Page" and not(title.startswith("Special:"))):		
		return True

def to_int(tupleValue):
	temp = list(tupleValue)
	temp[3] = int(temp[3])
	return tuple(temp)
	
def to_key_value_tuple(arg):
	return (arg[0], (arg[3], arg[2]))

#main method
def main():
    text = sc.textFile(inputs)
	words = text.flatMap(wikipedia_words).map(to_int).filter(filter_pages)
	var = words.map(to_key_value_tuple)
	wordcount = var.reduceByKey(max)		

	max_count = wordcount.sortBy(get_key)
	max_count.map(tab_separated).saveAsTextFile(output)

if __name__ == "__main__":
    main()	


