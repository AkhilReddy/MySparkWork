
#Using Spark Streaming for creating a system to enrich incoming data from a cloudant database. Example -

'''
Incoming Message: {"id" : 123}
Outgoing Message: {"id" : 123, "data": "xxxxxxxxxxxxxxxxxxx"}
'''

#Code for the driver class is as follows:


from Sample.Job import EnrichmentJob
from Sample.Job import FunctionJob
import pyspark
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession

from kafka import KafkaConsumer, KafkaProducer
import json

class SampleFramework():

    def __init__(self):
        pass

    @staticmethod
    def messageHandler(m):
        return json.loads(m.message)

    @staticmethod
    def processData(rdd):

        if (rdd.isEmpty()):
            print("RDD is Empty")
            return

        # Expand
        expanded_rdd = rdd.mapPartitions(EnrichmentJob.enrich)

        # Score
        scored_rdd = expanded_rdd.map(FunctionJob.function)

        # Publish RDD


    def run(self, ssc):

        self.ssc = ssc

        directKafkaStream = KafkaUtils.createDirectStream(self.ssc, QUEUENAME, \
                                                          {"metadata.broker.list": META, 
                                                          "bootstrap.servers": SERVER}, \
                                                          messageHandler= SampleFramework.messageHandler)

        directKafkaStream.foreachRDD(SampleFramework.processData)

        ssc.start()
        ssc.awaitTermination()


##############################  Code for the the Enrichment Job is as follows: class EnrichmentJob: ############################################


cache = {}

@staticmethod
def enrich(data):

    # Assume that Cloudant Connector using the available config
    cloudantConnector = CloudantConnector(config, config["cloudant"]["host"]["req_db_name"])
    final_data = []
    for row in data:
        id = row["id"]
        if(id not in EnrichmentJob.cache.keys()):
            data = cloudantConnector.getOne({"id": id})
            row["data"] = data
            EnrichmentJob.cache[id]=data
        else:
            data = EnrichmentJob.cache[id]
            row["data"] = data
        final_data.append(row)

    cloudantConnector.close()

    return final_data


################################################################################################################################################

Is there someway to maintain [1]"a global cache on the main memory that is accessible to all workers"
No. There is no "main memory" which can be accessed by all workers. Each worker runs in a separate process and communicates with external world with sockets. Not to mention separation between different physical nodes in non-local mode.

There are some techniques that can be applied to achieve worker scoped cache with memory mapped data (using SQLite being the simplest one) but it takes some additional effort to implement the right way (avoid conflicts and such).

or [2]"local caches on each of the workers such that they remain persisted in the foreachRDD setting"?
You can use standard caching techniques with scope limited to the individual worker processes. Depending on the configuration (static vs. dynamic resource allocation, spark.python.worker.reuse) it may or may not be preserved between multiple tasks and batches.

Consider following, simplified, example:

map_param.py:
from pyspark import AccumulatorParam
from collections import Counter

class CounterParam(AccumulatorParam):
    def zero(self, v: Counter) -> Counter:
        return Counter()

    def addInPlace(self, acc1: Counter, acc2: Counter) -> Counter:
        acc1.update(acc2)
        return acc1
my_utils.py:
from pyspark import Accumulator
from typing import Hashable
from collections import Counter

# Dummy cache. In production I would use functools.lru_cache 
# but it is a bit more painful to show with accumulator
cached = {} 

def f_cached(x: Hashable, counter: Accumulator) -> Hashable:
    if cached.get(x) is None:
        cached[x] = True
        counter.add(Counter([x]))
    return x


def f_uncached(x: Hashable, counter: Accumulator) -> Hashable:
    counter.add(Counter([x]))
    return x
main.py:
from pyspark.streaming import StreamingContext
from pyspark import SparkContext

from counter_param import CounterParam
import my_utils

from collections import Counter

def main():
    sc = SparkContext("local[1]")
    ssc = StreamingContext(sc, 5)

    cnt_cached = sc.accumulator(Counter(), CounterParam())
    cnt_uncached = sc.accumulator(Counter(), CounterParam())

    stream = ssc.queueStream([
        # Use single partition to show cache in work
        sc.parallelize(data, 1) for data in
        [[1, 2, 3], [1, 2, 5], [1, 3, 5]]
    ])

    stream.foreachRDD(lambda rdd: rdd.foreach(
        lambda x: my_utils.f_cached(x, cnt_cached)))
    stream.foreachRDD(lambda rdd: rdd.foreach(
        lambda x: my_utils.f_uncached(x, cnt_uncached)))

    ssc.start()
    ssc.awaitTerminationOrTimeout(15)
    ssc.stop(stopGraceFully=True)

    print("Counter cached {0}".format(cnt_cached.value))
    print("Counter uncached {0}".format(cnt_uncached.value))

if __name__ == "__main__":
    main()
Example run:

bin/spark-submit main.py
Counter cached Counter({1: 1, 2: 1, 3: 1, 5: 1})
Counter uncached Counter({1: 3, 2: 2, 3: 2, 5: 2})
As you can see we get expected results:

For "cached" objects accumulator is updated only once per unique key per worker process (partition).
For not-cached objects accumulator is update each time key occurs.

