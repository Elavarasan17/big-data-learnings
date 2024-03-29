#import statements
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
from datetime import datetime
import os, gzip
import uuid
import sys, re

#regex compiler
line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')

#Input Log Input
#['205.199.120.118', '01/Aug/1995:00:36:15', '/cgi-bin/imagemap/countdown70?59,186', '96']
def main(input, outKeySpace, table):
    #initial connection
    cluster = Cluster(['node1.local', 'node2.local'])
    session = cluster.connect(outKeySpace)
    
    insertStatement = session.prepare("INSERT INTO " + table + " (id, hostname, datetime, path, bytes) VALUES (?, ?, ?, ?, ?)")
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    batchSizeValue = 0
    session.execute("TRUNCATE " + table)
    
    #unzip the files and load the data
    for f in os.listdir(input):
        with gzip.open(os.path.join(input, f), 'rt', encoding='utf-8') as logfile:
            for line in logfile:
                values = line_re.split(line)[1:-1]

                if len(values) == 4:
                    datetimevalue = datetime.strptime(values[1], '%d/%b/%Y:%H:%M:%S')
                    batch.add(insertStatement, (uuid.uuid4(), values[0], datetimevalue, values[2], int(values[3])))
                    batchSizeValue += 1

                    if batchSizeValue == 300:
                        session.execute(batch)
                        batch.clear()
                        batchSizeValue = 0
    session.execute(batch)

    #rows = session.execute('SELECT path, bytes FROM nasalogs WHERE hostname=%s', ['grimnet23.idirect.com'])

#calling main method
if __name__ == '__main__':
    inputDir = sys.argv[1]
    keyspace = sys.argv[2]
    tableName = sys.argv[3]
    main(inputDir, keyspace, tableName)
