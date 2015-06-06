#!/bin/bash

# Description: This job indexes images from the UPCReplica dataset

##############################################################
################# Default parameters #########################
##############################################################

PERSISTENCE=1 # Persistence mode. Set to 0 if you want to index and query files on memory and 1 for disk (HBase).
RESET=false # Whether to flush data before indexing the images

##############################################################
##### Important parameters - Those that need to be changed ###
##############################################################

DATASET_PATH=/path/to/UPCReplica/dataset
DATASET_SRC_FILE=/path/to/base.csv
HBASE_MASTER=127.0.0.1 # Path to the hbase master. Only needed for 'disk' persistence 
ZOOKEEPER_PORT=2181 # Zookeeper port. Only needed for 'disk' persistence 
ZOOKEEPER_HOST=127.0.0.1 # Zookeeper manager host. Only needed for 'disk' persistence 
SPARK_MASTER=local[2] # Recommended to use as many executors as CPUs. 
MEM_FILE=/path/to/config.conf # Memory configuration file path. Only valid for 'memory' persistence mode. Set to null otherwise
OPENCV_PATH=/path/to/opencv-2.4.9/lib
SPARK_BIN=/path/to/spark/bin

#############################################################
########## Submition to Apache Spark ########################
#############################################################

$SPARK_BIN/spark-submit --class org.twitterReplica.jobs.IndexingJobReset --master $SPARK_MASTER --driver-library-path $OPENCV_PATH target/replica-0.0.1-SNAPSHOT-jar-with-dependencies.jar $PERSISTENCE $DATASET_PATH $DATASET_SRC_FILE $RESET $MEM_FILE $HBASE_MASTER $ZOOKEEPER_PORT $ZOOKEEPER_HOST
