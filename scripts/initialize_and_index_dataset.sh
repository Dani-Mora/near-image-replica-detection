#!/bin/bash

# Description: Initializes the detector (data contained is flushed) and stores the images from the UPCReplica dataset

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

##############################################################
##### Important parameters - Those that need to be changed ###
##############################################################

HBASE_MASTER=127.0.0.1 # Path to the hbase master. Only needed for 'disk' persistence 
ZOOKEEPER_PORT=2181 # Zookeeper port. Only needed for 'disk' persistence 
ZOOKEEPER_HOST=127.0.0.1 # Zookeeper manager host. Only needed for 'disk' persistence 
SPARK_MASTER=local[2] # Recommended to use as many executors as CPUs. 
MEM_FILE=/path/to/file.conf # Memory configuration file path. Only valid for 'memory' persistence mode. Set to null otherwise
OPENCV_PATH=/path/to/opencv/lib
DATASET_PATH=/path/to/UPCReplica/dataset
DATASET_SRC_FILE=/path/to/base.csv # An example of this file can be found in github
SPARK_BIN=/path/to/spark/bin
PARTITIONS=10 # Number of partitions to use

#############################################################
########## Submition to Apache Spark ########################
#############################################################

$SPARK_BIN/spark-submit --class org.twitterReplica.jobs.IndexingJobReset --master $SPARK_MASTER --driver-library-path $OPENCV_PATH target/replica-0.0.1-SNAPSHOT-jar-with-dependencies.jar $DESCRIPTOR_TYPE $KEYPOINT_TYPE $SIDE $FEATURE_FILTERING $FEATURE_THRESH $LOG_SCALE $NUM_TABLES $W $HAMMING_THRESH $ENCODING $COMPRESSION $TTL $PERSISTENCE $DATASET_PATH $DATASET_SRC_FILE $MEM_FILE $HBASE_MASTER $ZOOKEEPER_PORT $ZOOKEEPER_HOST $PARTITIONS
