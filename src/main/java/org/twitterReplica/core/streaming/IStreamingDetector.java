package org.twitterReplica.core.streaming;

import org.apache.spark.streaming.api.java.JavaStreamingContext;

public interface IStreamingDetector {

	/*
	 * 	Indexes the tweets extracted from the tweets received by the Spark Streaming API and given the OAuth
	 * 	credentials.
	 * 	@param spark Java Streaming Context
	 * 	@param Twitter consumer key
	 * 	@param consumerSecret Twitter consumer secret
	 * 	@param accessToken Twitter access token
	 * 	@param accessTokenSecret Twitter's access token secret
	 */
	public void indexStreaming(JavaStreamingContext spark, String consumerKey, String consumerSecret,
			String accessToken, String accessTokenSecret);
	
	/*
	 * 	Requests queries into the replica detection system using the Spark Streaming API and given the OAuth
	 * 	credentials.
	 * 	@param spark Java Streaming Context
	 * 	@param Twitter consumer key
	 * 	@param consumerSecret Twitter consumer secret
	 * 	@param accessToken Twitter access token
	 * 	@param accessTokenSecret Twitter's access token secret
	 * 	@param rank Weight threshold for weight filtering. To disable it, set this to negative
	 */
	public void queryStreaming(JavaStreamingContext spark, String consumerKey, String consumerSecret,
			String accessToken, String accessTokenSecret, int rank);
	
	/*
	 * 	Requests queries into the replica detection system using the Spark Streaming API and given the OAuth
	 * 	credentials and index them afterwards
	 * 	@param spark Java Streaming Context
	 * 	@param Twitter consumer key
	 * 	@param consumerSecret Twitter consumer secret
	 * 	@param accessToken Twitter access token
	 * 	@param accessTokenSecret Twitter's access token secret
	 * 	@param rank Weight threshold for weight filtering. To disable it, set this to negative
	 */
	public void indexAndQueryStreaming(JavaStreamingContext spark, String consumerKey, String consumerSecret,
			String accessToken, String accessTokenSecret, int rank);
}
