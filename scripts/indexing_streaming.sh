#!/bin/bash

# Description: Indexes images received by the Twitter Streaming API

# Twitter libs - Do not change
TWITTER4J_CORE=lib/twitter4j-core-3.0.3.jar
TWITTER4J_STREAMING=lib/twitter4j-stream-3.0.3.jar

##############################################################
################# Default parameters #########################
##############################################################

DURATION=1 # Size in seconds of the batches of the stream arriving
PER_SECOND=2 # Ratio of tweets received per second
PERSISTENCE=1 # Persistence mode. Set to 0 if you want to index and query files on memory and 1 for disk (HBase).

##############################################################
##### Important parameters - Those that need to be changed ###
##############################################################

HBASE_MASTER=127.0.0.1 # Path to the hbase master. Only needed for 'disk' persistence 
ZOOKEEPER_PORT=2181 # Zookeeper port. Only needed for 'disk' persistence 
ZOOKEEPER_HOST=127.0.0.1 # Zookeeper manager host. Only needed for 'disk' persistence 
SPARK_MASTER=local[2] # Recommended to use as many executors as CPUs. 
MEM_FILE=/path/to/file.conf # Memory configuration file path. Only valid for 'memory' persistence mode. Set to null otherwise
OPENCV_PATH=/path/to/opencv-2.4.9//lib
SPARK_BIN=/path/to/spark/bin

# Twitter Credentials
CONSUMER_KEY="insert_consumer_key" # Twitter consumer key 
CONSUMER_SECRET="insert_consumer_secret" # Twitter consumer secret
ACCESS_TOKEN="insert_access_token" # Twitter access token
ACCESS_TOKEN_SECRET="insert_access_token_secret" # Twitter access token

#############################################################
########## Submition to Apache Spark ########################
#############################################################

$SPARK_BIN/spark-submit --class org.twitterReplica.jobs.streaming.IndexingStreamingJob --master $SPARK_MASTER --jars $TWITTER4J_CORE,$TWITTER4J_STREAMING --driver-library-path $OPENCV_PATH target/replica-0.0.1-SNAPSHOT-jar-with-dependencies.jar $DURATION $PER_SECOND $PERSISTENCE $MEM_FILE $HBASE_MASTER $ZOOKEEPER_PORT $ZOOKEEPER_HOST $CONSUMER_KEY $CONSUMER_SECRET $ACCESS_TOKEN $ACCESS_TOKEN_SECRET
