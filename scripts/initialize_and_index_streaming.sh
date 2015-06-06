#!/bin/bash

# Description: Initializes the detector (data is flushed) and indexes images 
# received by the Twitter Streaming API.

# Twitter libs - Do not change
TWITTER4J_CORE=lib/twitter4j-core-3.0.3.jar
TWITTER4J_STREAMING=lib/twitter4j-stream-3.0.3.jar

##############################################################
################# Default parameters #########################
##############################################################

DESCRIPTOR_TYPE=4 # Type of descriptor to compute around each keypoint. 0: SIFT, 1: SURF, 2: ORB, 3: FREAK, 4: BRISK, 5: BRIEF
KEYPOINT_TYPE=4 # Extractor technique to detect the ares of interest of the image. 0: SIFT, 1: SURF, 2: ORB, 3: MSER, 4: BRISK, 5: HARRIS
SIDE=350 # Maximum largest side of the image. If image is larger than that, will be resized.
FEATURE_FILTERING=1 # Type of feature filtering. 0: No filtering, 1: Entropy filtering, 2: Variance filtering
FEATURE_THRESH=4.6 # Threshold to use to discard features if filtering is enabled. Approximate recomended ranges:
					# SIFT: Entropy 5.1-5.9, Variance 600-1400.
					# SURF: Entropy 3.25-4.25,Variance 0.012-0.015.
					# ORB: Entropy 4.6-4.9, Variance 4000-6500.
					# FREAK: Entropy 5.4-5.9, Variance 3500-7000.
					# BRISK: Entropy 4.5-5.5, Variance 7000-11000.
					# BRIEF: Entropy 4.75-4.95,Variance 4000-7000.
LOG_SCALE=false # If log scaling is needed to be applied on the features. Recommended for SIFT and ORB descriptors.
NUM_TABLES=4 # Number of blocks to split the sketches into.
W=10 # Sensitivity hashing parameter. The bigger it is, the easier for features to match (and to generate false positives). Recommended: between 10 and 20.
HAMMING_THRESH=3 # Hamming distance threshold for feature matches.
ENCODING=false # Enable/Disable encoding of values in HBase for the hash tables and the image table to save space. Recommended to set as "false". Only used when disk persistence selected.
COMPRESSION=true # Enable/Disable compression on the tables. Recommended to set to "true". Only used when disk persistence selected.
TTL=-1 # Whether we want rows in the database to expire after some specific amount of seconds. Set to negative if you want them to persist. Only used when disk persistence selected.
PERSISTENCE=1 # Persistence mode. Set to 0 if you want to index and query files on memory and 1 for disk (HBase).

DURATION=1 # Size in seconds of the batches of the stream arriving
PER_SECOND=2 # Ratio of tweets received per second

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
# Twitter credentials
CONSUMER_KEY="insert_consumer_key" # Twitter consumer key 
CONSUMER_SECRET="insert_consumer_secret" # Twitter consumer secret
ACCESS_TOKEN="insert_access_token" # Twitter access token
ACCESS_TOKEN_SECRET="insert_access_token_secret" # Twitter access token

#############################################################
########## Submition to Apache Spark ########################
#############################################################

$SPARK_BIN/spark-submit --class org.twitterReplica.jobs.streaming.IndexingStreamingJobReset --master $SPARK_MASTER --jars $TWITTER4J_CORE,$TWITTER4J_STREAMING --driver-library-path $OPENCV_PATH target/replica-0.0.1-SNAPSHOT-jar-with-dependencies.jar $DESCRIPTOR_TYPE $KEYPOINT_TYPE $SIDE $FEATURE_FILTERING $FEATURE_THRESH $LOG_SCALE $NUM_TABLES $W $HAMMING_THRESH $ENCODING $COMPRESSION $TTL $PERSISTENCE $DURATION $PER_SECOND $MEM_FILE $HBASE_MASTER $ZOOKEEPER_PORT $ZOOKEEPER_HOST $CONSUMER_KEY $CONSUMER_SECRET $ACCESS_TOKEN $ACCESS_TOKEN_SECRET
