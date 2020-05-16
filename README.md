# Kafka Streams Java Challenge

The goal of this challenge is to develop a program that would allow us to process log frames in json format containing a ts and uid fields, ts being a unix timestamp, uid being a user id.  

We should produce the count of unique users per minute, in a Kafka topic, in json format.

We are given a program that allows us to generate log frames in json format, with the expected ts, uid and other fields that we don't need to process.
  
We will use this tool to inject the log frames produced in a Kafka topic, allowing us to focus on creating the Kafka consumer only.  

## Log frames

We start here by generating a log frame to get a better idea of what we can expect, because even if the log frame generator tool allows us to specify the count of log frames we want to generate, we still need to have an idea of :  
* the structure of a log frame
* what is the format of the unix timestamp for the ts field
* what is the format of a uid field

So after generating some log frames with the provided tool, we can see that the ts and uid fields are nodes located at the
root level of the json document, so it will be easy to extract them.

Below is an example of a log frame, without the redundant fields :  
```
{"ts":1468244384,"uid":"9ce8499df95b4fff567"}
```

It is easily produced from the bigger log frames file using the jq tool :  

```
cat logframes.txt | jq '{ ts: .ts, uid: .uid }'
```

### Better understanding the log frames and getting some metrics

It is good to get an idea of how many unique users we have per minute, so that we can record this metrics
and compare them with the metrics we will produce with our Kafka Consumer, allowing us to feel confident about our development.

Here are a few options to accomplish this :

* we can use the jq tool to extract only the ts and uid fields from the log frames file, and then process this values under unix
* we can use the spark console to read the log frames file, and produce the expected metrics

We will use the spark console for this purpose, so let's run the spark-shell

```
spark-shell
```

Then enter the following code, that :
* loads the log frames
* converts the ts unix timestamp to a date format excluding the seconds (tsMinute)
* then grouping the records by the tsMinute field
* then aggregating the values keeping only the count of distinct uids

```
val df = spark.read.format("json").load("../../../projects/alpiq/stream_ts_uid.json")
df.printSchema  
val metricsDf = df.withColumn("tsMinute", expr("from_unixtime(ts, 'YYYY-MM-dd HH:mm')")).groupBy("tsMinute").agg(countDistinct("uid").as("uniqueUsers"))
metricsDf.show
```

The result is the following for our log frame example, and this are the values we will be expecting from our Kafka consumer :

```
+----------------+-----------+                                                  
|        tsMinute|uniqueUsers|
+----------------+-----------+
|2016-07-11 14:39|      10818|
|2016-07-11 14:40|      46580|
|2016-07-11 14:41|      54416|
|2016-07-11 14:42|      46117|
|2016-07-11 14:43|      50967|
|2016-07-11 14:44|      51930|
|2016-07-11 14:45|      43799|
|2016-07-11 14:46|      46113|
|2016-07-11 14:47|      50431|
|2016-07-11 14:48|      47483|
|2016-07-11 14:49|      52616|
|2016-07-11 14:50|      51188|
|2016-07-11 14:51|      41031|
|2016-07-11 14:52|      42176|
|2016-07-11 14:53|      49222|
|2016-07-11 14:54|      45260|
|2016-07-11 14:55|       5211|
+----------------+-----------+
```

## Creating and feeding our input Kafka topic

As already mentioned before, we will use the Kafka console producer tool to inject the log frames in our topic.  

Kafka being distributed by nature, we would like to benefit of its capabilities, and one of the things we should care about from the beginning is the partitioning of the topic our producer will feed, nd the distribution of our data amongst our partitions.

We would like to have more than one partition in our input topic, so this means that we have to specify a key when injecting our data in the input topic using the kafka console producer.
We will use the ts field as our key, allowing us to have all the log frames with the same ts value in the same partition.

### Creating our input Kafka topic

```
kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic log-frames --create --partitions 4 --replication-factor 1
```

### Feeding our input Kafka topic

We will use the jq tool to ease the process, the command below allows to inject the log frames in our input topic and adds a key to each record,   
the key being the conversion of the ts field from the json log frame record, with a minute granularity.

This allows of course to have all the records with the same key in the same partition, and this will prove useful as we want to count unique
users per minute. 

In the real life, we would expect the producer to do this automatically for us, while sending records to the input topic.

```
cat stream.jsonl | jq -r '[(.ts | strftime("%Y-%m-%d %H:%M")),.] | "\(.[0])~\(.[1])"' | kafka-console-producer.sh \
--broker-list localhost:9092 \
--topic log-frames \
--property "parse.key=true" \
--property "key.separator=~"
```

