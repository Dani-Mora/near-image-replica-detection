package org.twitterReplica.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.twitterReplica.data.hbase.HBaseClient;
import org.twitterReplica.exceptions.DataException;
import org.twitterReplica.exceptions.NotFoundException;
import org.twitterReplica.model.ImageFeature;
import org.twitterReplica.model.ImageInfo;
import org.twitterReplica.spark.indexing.IndexImagesPartition;
import org.twitterReplica.spark.indexing.streaming.IndexStreamingHBase;
import org.twitterReplica.spark.query.disk.QueryFeaturesPartition;

public class DiskPersistenceSystem implements IPersistence {

	@Override
	public DescriptorParams readDescriptorParams(ReplicaConnection params,
			JavaSparkContext spark) throws NotFoundException, DataException {
		Configuration config = HBaseClient.getConfiguration(params);
		return HBaseClient.readDescriptorParams(config);
	}

	@Override
	public IndexingParams readIndexingParams(ReplicaConnection params,
			JavaSparkContext spark, int featureSize) throws NotFoundException, DataException {
		Configuration config = HBaseClient.getConfiguration(params);
		return HBaseClient.readIndexingParams(config);
	}

	@Override
	public FilteringParams readFilteringParams(ReplicaConnection params,
			JavaSparkContext spark) throws NotFoundException, DataException {
		Configuration config = HBaseClient.getConfiguration(params);
		return HBaseClient.readFilteringParams(config);
	}
	
	@Override
	public void restartData(ReplicaConnection params, IndexingParams ind) throws DataException {
		Configuration conf = HBaseClient.getConfiguration(params);
		HBaseClient.resetData(conf, ind);
	}

	@Override
	public void storeParameters(ReplicaConnection cParams, DescriptorParams dParams, IndexingParams iParams,
			FilteringParams fParams) throws DataException {
		Configuration conf = HBaseClient.getConfiguration(cParams);
		HBaseClient.storeParameters(conf, dParams, fParams, iParams);
	}
	
	@Override
	public void onConnected() {
		// Nothing to do
	}

	@Override
	public void indexData(ReplicaConnection cParams, IndexingParams ind, JavaPairRDD<ImageInfo, ImageFeature> sketches) {
		JavaPairRDD<ImageInfo, Iterable<ImageFeature>> groups = sketches.groupByKey();
		groups.foreachPartition(new IndexImagesPartition(ind, cParams));
	}

	@Override
	public void indexStreaming(ReplicaConnection cParams, IndexingParams ind,
			JavaPairDStream<ImageInfo, ImageFeature> sketches) {
		JavaPairDStream<ImageInfo, Iterable<ImageFeature>> imgs = sketches.groupByKey();
		imgs.foreachRDD(new IndexStreamingHBase(ind, cParams));
	}

	@Override
	public JavaPairDStream<ImageFeature, ImageFeature> queryFeaturesStreaming(ReplicaConnection cParams, IndexingParams params,
			JavaPairDStream<ImageInfo, ImageFeature> sketches) {
		return sketches.mapPartitionsToPair(new QueryFeaturesPartition(params, cParams));
	}
	
	@Override
	public JavaPairRDD<ImageFeature, ImageFeature> queryFeatures(
			ReplicaConnection conn, IndexingParams params,
			JavaPairRDD<ImageInfo, ImageFeature> sketches) {
		return sketches.mapPartitionsToPair(new QueryFeaturesPartition(params, conn));
	}

	@Override
	public ImageInfo getImage(ReplicaConnection cParams, long id) throws DataException {
		Configuration conf = HBaseClient.getConfiguration(cParams);
		return HBaseClient.getImage(conf, id);
	}

}
