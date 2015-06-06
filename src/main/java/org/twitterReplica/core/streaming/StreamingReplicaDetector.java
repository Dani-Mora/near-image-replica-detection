package org.twitterReplica.core.streaming;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.twitterReplica.core.ReplicaConnection;
import org.twitterReplica.core.ReplicaSystem;
import org.twitterReplica.exceptions.DataException;
import org.twitterReplica.model.FilteringType;
import org.twitterReplica.model.ImageFeature;
import org.twitterReplica.model.ImageInfo;
import org.twitterReplica.model.ImageMatch;
import org.twitterReplica.model.PersistenceMode;
import org.twitterReplica.model.providers.ProviderType;
import org.twitterReplica.spark.features.ComputeFeatures;
import org.twitterReplica.spark.features.EntropyFiltering;
import org.twitterReplica.spark.features.LogScaleFunction;
import org.twitterReplica.spark.features.SketchProcedure;
import org.twitterReplica.spark.features.VarianceFiltering;
import org.twitterReplica.spark.query.HammingFiltering;
import org.twitterReplica.spark.query.WeightFiltering;
import org.twitterReplica.spark.query.streaming.MatchExtractorStreaming;
import org.twitterReplica.spark.streaming.TweetsToImagesTask;

import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

public class StreamingReplicaDetector extends ReplicaSystem implements IStreamingDetector {

	public StreamingReplicaDetector(PersistenceMode mode, ReplicaConnection conf) {
		super(mode, conf);
	}

	@Override
	public void indexStreaming(JavaStreamingContext spark, String consumerKey,
			String consumerSecret, String accessToken, String accessTokenSecret) {
		
		// Get stream
		JavaReceiverInputDStream<Status> tweets = getTwitterStream(spark, consumerKey, consumerSecret, 
				accessToken, accessTokenSecret);
		// all indexing
		indexTweets(tweets);
	}
	
	@Override
	public void queryStreaming(JavaStreamingContext spark, String consumerKey,
			String consumerSecret, String accessToken, String accessTokenSecret, final int rank) {
		// Get stream
		JavaReceiverInputDStream<Status> tweets = getTwitterStream(spark, consumerKey, consumerSecret, 
				accessToken, accessTokenSecret);
		// Call query
		queryTweets(tweets, rank);
	}
	

	@Override
	public void indexAndQueryStreaming(JavaStreamingContext spark, String consumerKey, 
			String consumerSecret, String accessToken, String accessTokenSecret, int rank) {
		// Get stream
		JavaReceiverInputDStream<Status> tweets = getTwitterStream(spark, consumerKey, consumerSecret, 
				accessToken, accessTokenSecret);
		// Call query
		queryTweets(tweets, rank);
		// Call indexing
		indexTweets(tweets);
		
	}
	
	/*
	 * 	Indexes the input tweets into the system
	 * 	@param tweets Input tweets
	 */
	protected void indexTweets(JavaReceiverInputDStream<Status> tweets) {
		JavaPairDStream<ImageInfo, ImageFeature> imFeatures = computeImageFeatures(tweets);
		// Build sketches
		JavaPairDStream<ImageInfo, ImageFeature> sketches = 
				imFeatures.mapValues(new SketchProcedure(indParams.getSketchFunction(), indParams.getNumTables()));
		// Index information
		system.indexStreaming(conn, indParams, sketches);
	}
	
	/*
	 * 	Indexes the input tweets into the system
	 * 	@param tweets Input tweets
	 */
	protected void queryTweets(JavaReceiverInputDStream<Status> tweets, int rank) {
		
		// Compute sketches
		JavaPairDStream<ImageInfo, ImageFeature> imFeatures = computeImageFeatures(tweets);
		
		JavaPairDStream<ImageInfo, ImageFeature> sketches = imFeatures.mapValues(new SketchProcedure(indParams.getSketchFunction(), 
				indParams.getNumTables()));
		
		// Query specific and filter by hamming distance
		JavaPairDStream<ImageFeature, ImageFeature> candidates = system.queryFeaturesStreaming(conn,indParams, sketches);
		JavaPairDStream<ImageFeature, ImageFeature> filteredHamming = 
				candidates.filter(new HammingFiltering(indParams.getHammingDistance()));
		
		// Group by image and assign weights
		JavaDStream<ImageMatch> matchedIds = filteredHamming.map(new MatchExtractorStreaming());
		JavaPairDStream<ImageMatch, Long> result = matchedIds.countByValue();

		// Filter by weight if requested
		if (rank > 0) {
			result = result.filter(new WeightFiltering(rank));
		}
		
		// Print results
		result.print();
	}
	
	/*
	 * 	Returns a ready-to-use Twitter stream using the input credentials
	 * 	@param spark Java Streaming Context
	 * 	@param Twitter consumer key
	 * 	@param consumerSecret Twitter consumer secret
	 * 	@param accessToken Twitter access token
	 * 	@param accessTokenSecret Twitter's access token secret
	 * 	@return Stream of tweets
	 */
	protected static JavaReceiverInputDStream<Status> getTwitterStream(JavaStreamingContext spark, String consumerKey, String consumerSecret,
			String accessToken, String accessTokenSecret) {
		// Enable Oauth
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		  .setOAuthConsumerKey(consumerKey).setOAuthConsumerSecret(consumerSecret)
		  .setOAuthAccessToken(accessToken).setOAuthAccessTokenSecret(accessTokenSecret);
		TwitterFactory tf = new TwitterFactory(cb.build());
		Twitter twitter = tf.getInstance();
		
		// Create stream
		return TwitterUtils.createStream(spark, twitter.getAuthorization());
	}
	
	/*
	 * 	Computes the image features from the input tweets
	 * 	@param tweets Input stream of tweets
	 * 	@return Stream of sketched image features after filtered and scaling of the features from the input tweets
	 */
	protected JavaPairDStream<ImageInfo, ImageFeature> computeImageFeatures(JavaReceiverInputDStream<Status> tweets) {
		
		JavaDStream<ImageInfo> imgs = tweets.mapPartitions(new TweetsToImagesTask());
		
		JavaPairDStream<ImageInfo, ImageFeature> features = imgs.flatMapToPair(new ComputeFeatures(descParams, ProviderType.TWITTER));
		JavaPairDStream<ImageInfo, ImageFeature> filtered = features;
		
		// Filter descriptors if needed
		if (filtParams.getFilteringType().equals(FilteringType.ENTROPY)) {
			filtered = features.filter(new EntropyFiltering(filtParams.getThresh()));
		}
		else if (filtParams.getFilteringType().equals(FilteringType.VARIANCE)) {
			filtered = features.filter(new VarianceFiltering(filtParams.getThresh()));
		}
		
		// Logscale features if needed
		if (filtParams.isLogScaleEnabled()) {
			filtered = filtered.mapValues(new LogScaleFunction());
		}
		
		// Build sketches
		return filtered;
	}

	@Override
	public ImageInfo getImage(long id) throws DataException {
		return system.getImage(conn, id);
	}
	
}
