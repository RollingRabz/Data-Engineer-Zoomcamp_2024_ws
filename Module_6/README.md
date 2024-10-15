# Stream Processing with Kafka
## What is Kafka
Apache Kafka is an open-source distributed event streaming platform used by thousands of companies for high-performance data pipelines, streaming analytics, data integration, and mission-critical applications.
## Kafka components
- Topic
  
  Topic is the continuous stream of events.
  
- Event
  
  Event is item we store in topic. Ex. Water temperature recorded every 30 seconds. The first 30 seconds will be event 1 and the next 30 seconds will be events 2.
  
- Logs
  
  Logs is the way we store event in topic.

In Kafka, an event is contained in a message, which consists of three components: Key, Value, and Timestamp.

## Benefit of Kafka compare to other

* Provided robustness and reliability to topic.

  If the server is down you will still able to receive the data since kafka provided replication function.

* Flexibility

  Can connect to many consumers.

* Scalability

  Can be scale up or down easily.

## How Kafka exchange data

1. Produce message to Kafka topic.

2. Consumer register to interested topic

3. Now consumercan read the register topic.

## Kafka cluster

Machine that running kafka which talking together

## Reliable

* Replication

  Assume we set replication = 2 and we have 3 node A, B, and C. First we store data at node A and replication at B then A dead B will lead instead of A. After that B will replicate to C to make sure that replication = 2.

* Retention

  How long we will keep this  data. Ex. 1 day, 1 week

* Partition

  The more partition will allow more consumer to connect which reduce the workload.

  Assume we have 2 partitions. If we have only 1 consumer reading from 2 partitions, the delay will occur due to API read delay time. To solve this problem we will add another consumer to reduce the workload.

## Offset

How does the consumer know which message to consume, and how does Kafka know that the consumer has already consumed a message and should consume the next one.

* It save consumer.group.id, topic, partition, offset in the internal kafka topic (--consumer_offset_)

## Auto offset reset

* Earliest

  Read from start

* Latest

  Assume new consumer group id join at event 100, it will read from 101 onward and ignore the first 100 events.

## Acknowledgement

0 : Succes immediately after send didn't check if leader or follower physically receive the message. (Fastest)

1: Success if leader physically received the message

all: Success if both leader and follower physically received the message. (Slowest)

## How message store in each partitions

* Key %(modulo) num of partitions

  This may cause some partition to have more message than other (skewed prob) if some key have more duplicate. Ex. use location as key Airport, School, Police dept,....

* If key = null then use round robin method(0 > 1 > 0 > 1 .....)

## Kafka stream join

Both topic must have the same number of partitions to prevent keys mismatch.

## Global KTable

The concept is similar to Spark's broadcast, where a small file is sent to every node without incurring any reshuffling costs.







