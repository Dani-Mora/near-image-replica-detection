#!/bin/bash

# Description: This job indexes the images from the input folder and appends this data 
# to the already indexed one
# It assumes that:
#	- There is already data stored in the system and parameters were also set
#	- Identifier of the image is given by its name. So ALL FILES MUST HAVE A NUMERIC NAME

##############################################################
################# Default parameters #########################
##############################################################

PERSISTENCE=1 # Persistence mode. Set to 0 if you want to index and query files on memory and 1 for disk (HBase).
RESET=false # Whether to flush data indexed before starting

##############################################################
##### Important parameters - Those that need to be changed ###
##############################################################

FOLDER=/path/to/folder # Input folder where to get images from. Acceptes extensions: "bmp", "dib", "jpeg", "jpg", "jpe", "jp2", "png", "pbm", "pgm", "ppm", "sr", "ras", "tiff", "tif"
HBASE_MASTER=127.0.0.1 # Path to the hbase master. Only needed for 'disk' persistence 
ZOOKEEPER_PORT=2181 # Zookeeper port. Only needed for 'disk' persistence 
ZOOKEEPER_HOST=127.0.0.1 # Zookeeper manager host. Only needed for 'disk' persistence 
SPARK_MASTER=local[2] # Recommended to use as many executors as CPUs. 
MEM_FILE=/path/to/file.conf # Memory configuration file path. Only valid for 'memory' persistence mode. Set to null otherwise
OPENCV_PATH=/path/to/opencv/lib
SPARK_BIN=/path/to/spark
PARTITIONS=10 # Number of partitions to use

#############################################################
########## Submition to Apache Spark ########################
#############################################################

$SPARK_BIN/spark-submit --class org.twitterReplica.jobs.IndexFolderJob --master $SPARK_MASTER --driver-library-path $OPENCV_PATH target/replica-0.0.1-SNAPSHOT-jar-with-dependencies.jar $PERSISTENCE $FOLDER $RESET $MEM_FILE $HBASE_MASTER $ZOOKEEPER_PORT $ZOOKEEPER_HOST $PARTITIONS
