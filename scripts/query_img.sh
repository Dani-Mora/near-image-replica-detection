#!/bin/bash

# Description: Queries a single image into the system

##############################################################
################# Default parameters #########################
##############################################################

PERSISTENCE=1 # Persistence mode. Set to 0 if you want to index and query files on memory and 1 for disk (HBase).
WEIGHT_FILT=2 # Weight filtering of the results. To disable it, set to negative.

##############################################################
##### Important parameters - Those that need to be changed ###
##############################################################

IMG=/path/to/image # Image to query
HBASE_MASTER=127.0.0.1 # Path to the hbase master. Only needed for 'disk' persistence 
ZOOKEEPER_PORT=2181 # Zookeeper port. Only needed for 'disk' persistence 
ZOOKEEPER_HOST=127.0.0.1 # Zookeeper manager host. Only needed for 'disk' persistence 
SPARK_MASTER=local[2] # Recommended to use as many executors as CPUs. 
MEM_FILE=/path/to/file.conf # Memory configuration file path. Only valid for 'memory' persistence mode. Set to null otherwise
OPENCV_PATH=/path/to/opencv/lib
SPARK_BIN=/path/to/spark/bin
PARTITIONS=10 # Number of partitions to use

#############################################################
########## Submition to Apache Spark ########################
#############################################################

$SPARK_BIN/spark-submit --class org.twitterReplica.jobs.QueryImageJob --master $SPARK_MASTER --driver-library-path $OPENCV_PATH target/replica-0.0.1-SNAPSHOT-jar-with-dependencies.jar $IMG $WEIGHT_FILT $PERSISTENCE $MEM_FILE $HBASE_MASTER $ZOOKEEPER_PORT $ZOOKEEPER_HOST $PARTITIONS
