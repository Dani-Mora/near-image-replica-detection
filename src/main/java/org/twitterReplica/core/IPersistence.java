package org.twitterReplica.core;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.twitterReplica.exceptions.DataException;
import org.twitterReplica.exceptions.NotFoundException;
import org.twitterReplica.model.ImageFeature;
import org.twitterReplica.model.ImageInfo;

public interface IPersistence {

	/*
	 * 	Reads the parameters related to the computation of descriptors
	 */
	public DescriptorParams readDescriptorParams(ReplicaConnection params, JavaSparkContext spark)
			throws NotFoundException, DataException;
	
	/*
	 * 	Reads the parameters regarding the indexing of the images
	 */
	public IndexingParams readIndexingParams(ReplicaConnection params, JavaSparkContext spark, 	
			int featureSize) throws NotFoundException, DataException;
	
	/*
	 * 	Reads the parameters related to the image filtering
	 */
	public FilteringParams readFilteringParams(ReplicaConnection params, JavaSparkContext spark)
			throws NotFoundException, DataException;
	
	/*
	 * 	Persists all parameters
	 */
	public void storeParameters(ReplicaConnection conn, DescriptorParams dParams, 
			IndexingParams iParams, FilteringParams fParams) throws DataException;
	
	/*
	 * 	Flushes all data and initializes all structures needed
	 */
	public void restart(ReplicaConnection params, IndexingParams ind) throws DataException;
	
	
	/*
	 * 	Restarts the data stored
	 */
	public void restartData(ReplicaConnection params, IndexingParams ind) throws DataException;
	
	/*
	 * 	Action to perform when a client has connected to the detector
	 */
	public void onConnected();
	
	/*
	 * 	Indexes the input feature sketches
	 */
	public void indexData(ReplicaConnection conn, IndexingParams ind, 
			JavaPairRDD<ImageInfo, ImageFeature> sketches);
	
	/*
	 * 	Indexes the input feature sketches
	 */
	public void indexStreaming(ReplicaConnection conn, IndexingParams ind, 
			JavaPairDStream<ImageInfo, ImageFeature> sketches);
	
	/*
	 * 	Queries image features and returns the pair of candidates
	 */
	/*public JavaPairRDD<ImageFeature, ImageFeature> queryFeatures(ReplicaConnection conn, IndexingParams params, 
			JavaRDD<ImageFeature> features);*/
	
	/*
	 * 	Queries image features and returns the pair of candidates
	 */
	public JavaPairRDD<ImageFeature, ImageFeature> queryFeatures(ReplicaConnection conn, IndexingParams params, 
			JavaPairRDD<ImageInfo, ImageFeature> sketches);
	
	/*
	 * 	Queries from image feature streams
	 */
	public JavaPairDStream<ImageFeature, ImageFeature> queryFeaturesStreaming(ReplicaConnection conn, IndexingParams params, 
			JavaPairDStream<ImageInfo, ImageFeature> sketches);
	
	/*
	 * 	@param conn Connection parameters
	 * 	@param id Image identifier
	 * 	@return Information related to image with given identifier
	 */
	public ImageInfo getImage(ReplicaConnection conn, long id) throws DataException;
	
}
