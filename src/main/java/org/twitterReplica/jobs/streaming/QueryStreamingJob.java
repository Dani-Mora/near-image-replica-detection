package org.twitterReplica.jobs.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.opencv.core.Core;
import org.twitterReplica.core.ReplicaConnection;
import org.twitterReplica.core.streaming.StreamingReplicaDetector;
import org.twitterReplica.exceptions.ConnectionException;
import org.twitterReplica.jobs.JobUtils;
import org.twitterReplica.model.PersistenceMode;

/*
 * 	Queries images received from the Twitter endpoint and prints the results
 */
public class QueryStreamingJob {

	public static void main(String[] args) {
		
		// Load OpenCV
		System.loadLibrary( Core.NATIVE_LIBRARY_NAME );
		
		int duration = (int) (Integer.valueOf(args[0]) * 1e3);
		int rank = Integer.valueOf(args[1]);
		PersistenceMode mode = JobUtils.readPersistence(Integer.valueOf(args[2]));
		int persec = Integer.valueOf(args[3]);
		String confFile = args[4];
		String hbaseMaster = args[5];
		int port = Integer.valueOf(args[6]);
		String zookeeperHost = args[7];
		String consumerKey = args[8];
		String consumerSecret = args[9];
		String accessToken = args[10];
		String accessTokenSecret = args[11];
		
		// Configure mode
		ReplicaConnection conn = mode.equals(PersistenceMode.DISK_ONLY) ?
				new ReplicaConnection(hbaseMaster, String.valueOf(port), zookeeperHost) :
				new ReplicaConnection(confFile, null, null);
		StreamingReplicaDetector detector = new StreamingReplicaDetector(PersistenceMode.DISK_ONLY, conn);
				
		// Create context
		SparkConf conf = new SparkConf();
		conf.set("spark.streaming.receiver.maxRate", String.valueOf(persec));
		final JavaStreamingContext spark = new JavaStreamingContext(conf, new Duration(duration));

		// Initialization from parameters
		try {
			detector.connect(conn, spark.sparkContext());
		} catch (ConnectionException e) {
			System.out.println("System initialization error: " + e.getMessage());
			System.exit(1);
		}
		
		// Schedule streaming
		detector.queryStreaming(spark, consumerKey, consumerSecret, 
				accessToken, accessTokenSecret, rank);

		// Avoid application to finish in unexpected state
		Runtime.getRuntime().addShutdownHook(new Thread() {
		   @Override
		   public void run() {
			   spark.stop(true, true);
		   }
		  });
		
		// Start spark and wait for ending
		spark.start();
		spark.awaitTermination();
	}

}
