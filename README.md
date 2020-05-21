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
val df = spark.read.format("json").load("../../../projects/alpiq/stream_ts_uid.json").withColumn("dateMinute", expr("from_unixtime(ts, 'YYYY-MM-dd HH:mm:00')"))
val metricsDf = df.groupBy("dateMinute").agg(countDistinct("uid").as("uniqueUsers")).withColumn("tsMinute", unix_timestamp(col("dateMinute"), "yyyy-MM-dd HH:mm:ss")).orderBy(col("dateMinute")).show
metricsDf.show
```

The result is the following for our log frame example, and this are the values we will be expecting from our Kafka consumer :

```
+-------------------+-----------+----------+                                    
|         dateMinute|uniqueUsers|  tsMinute|
+-------------------+-----------+----------+
|2016-07-11 14:39:00|      10818|1468244340|
|2016-07-11 14:40:00|      46580|1468244400|
|2016-07-11 14:41:00|      54416|1468244460|
|2016-07-11 14:42:00|      46117|1468244520|
|2016-07-11 14:43:00|      50967|1468244580|
|2016-07-11 14:44:00|      51930|1468244640|
|2016-07-11 14:45:00|      43799|1468244700|
|2016-07-11 14:46:00|      46113|1468244760|
|2016-07-11 14:47:00|      50431|1468244820|
|2016-07-11 14:48:00|      47483|1468244880|
|2016-07-11 14:49:00|      52616|1468244940|
|2016-07-11 14:50:00|      51188|1468245000|
|2016-07-11 14:51:00|      41031|1468245060|
|2016-07-11 14:52:00|      42176|1468245120|
|2016-07-11 14:53:00|      49222|1468245180|
|2016-07-11 14:54:00|      45260|1468245240|
|2016-07-11 14:55:00|       5211|1468245300|
+-------------------+-----------+----------+
```

## Creating and feeding our input Kafka topic

As already mentioned before, we will use the Kafka console producer tool to inject the log frames in our topic.  

Kafka being distributed by nature, we would like to benefit of its capabilities, and one of the things we should care about from the beginning is the partitioning of the topic our producer will feed, and the distribution of our data amongst our partitions.

We would like to have more than one partition in our input topic, so this means that we have to specify a key when injecting our data in the input topic using the kafka console producer.
We will use the ts field as our key, allowing us to have all the log frames with the same ts value in the same partition.

### Creating our input Kafka topic

Please see "Running the project"

### Feeding our input Kafka topic

We will use the jq tool to ease the process, the command below allows to inject the log frames in our input topic and adds a key to each record,   
the key being the conversion of the ts field from the json log frame record, with a minute granularity, as a unix timestamp of seconds.

This allows of course to have all the records with the same key in the same partition, and this will prove useful as we want to count unique
users per minute. 

In the real life, we would expect the producer to do this automatically for us, while sending records to the input topic.

// Not working due to time zone ; cat stream.jsonl | jq -r '[(.ts | strftime("%Y-%m-%dT%H:%M:00Z") | fromdate),.] | "\(.[0])~\(.[1])"' | kafka-console-producer.sh \
```
cat stream.jsonl | jq -r '[(60 * ((.ts / 60) | floor)),.] | "\(.[0])~\(.[1])"' | kafka-console-producer.sh \
--broker-list localhost:9092 \
--topic log-frames \
--property "parse.key=true" \
--property "key.separator=~"
```

# Running the project

In terms of architecture we will have :

* an input topic named log-frames
  * this topic contains many partitions
  * the events have a key whose value equals the ts field that we find in each event
* a consumer that reads events from the input topic :
  * groups the events per ts field value
  * applies a window of 1 minute to process the events
  * transforms the grouped values using a window store to exclude duplicated events based on the uid field
  * groups the remaining records by ts field
  * counts the uids (per ts field)
  * outputs the metrics in an output topic
* an output topic named unique-users-metrics
  * feeded by the consumer
  * unfortunately it contains the ifferent stages of aggregation for the production of the count of users
  * may be using kafka connect we could solve this problem ?
  * or another form of aggregator  

## Starting zookeeper and kafka

You could start zookeeper and kafka in 2 different terminal so that you can look at the log messages and check if anything fails.
The below commands have to be started from the kafka directory as the paths to the config files are relative.

```
zookeeper-server-start.sh config/zookeeper.properties

kafka-server-start.sh config/server.properties
```

## Creating the kafka topics

```
kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic log-frames --create --partitions 4 --replication-factor 1

kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic unique-users-metrics --create --partitions 4 --replication-factor 1
```

## Starting a consumer for the unique users metrics

```
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic unique-users-metrics --property print.key=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
```

## Running the project

Run the project from intellij or ```mvn package``` and run the .jar 

## Injecting test data

Running the script below allows to inject test data and visualize what is happening in the consumer
```
let idx=1
let ts=1468244340

while [ $ts -le 1468244530 ]
do
	js="{\"ts\":$ts,\"uid\":\"$idx\"}" 
	echo $js | \
		jq -r '[(60 * ((.ts / 60) | floor)),.] | "\(.[0])~\(.[1])"' | \
 		kafka-console-producer.sh --broker-list localhost:9092 --topic log-frames --property "parse.key=true" --property "key.separator=~"

	let ts=ts+10
	let idx=idx+1
	sleep 1
done 
```

## Measuring performance metrics

This could probably be achieved using jconsole / MBeans tab

# TODO

* Tests
* Produce the metrics as json
* Input/output topics count of partitions
* Purge the windowStores, they are persistent and it does not look like the retention period really works as the counts in deduplicate function always grow

