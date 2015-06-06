package org.twitterReplica.jobs.streaming;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.opencv.core.Core;
import org.twitterReplica.core.DescriptorParams;
import org.twitterReplica.core.FilteringParams;
import org.twitterReplica.core.ReplicaConnection;
import org.twitterReplica.core.streaming.StreamingReplicaDetector;
import org.twitterReplica.exceptions.InitializationException;
import org.twitterReplica.exceptions.InvalidArgumentException;
import org.twitterReplica.jobs.JobUtils;
import org.twitterReplica.model.PersistenceMode;

/*
 * 	Indexes images from the Twitter Streaming API given the parameters and erases all content stored so far
 */
public class IndexingStreamingJobReset {

	static Logger logger = Logger.getLogger(IndexingStreamingJobReset.class);
	
	public static void main(String[] args) {
		
		// Load OpenCV
		System.loadLibrary( Core.NATIVE_LIBRARY_NAME );
		
		// Read parameters
		DescriptorParams descParams = null;
		FilteringParams filtParams = null;
		try {
			descParams = JobUtils.readDescParamsFromInput(args);
			filtParams = JobUtils.readFiltParamsFromInput(args);
		} catch (InvalidArgumentException e) {
			System.out.println(e.getMessage());
			JobUtils.printIndexingResetUsage();
			System.exit(1);
		}
		
		int numTables = Integer.valueOf(args[6]);
		int W = Integer.valueOf(args[7]);
		int hammingThresh = Integer.valueOf(args[8]);
		boolean blockEncd = Boolean.valueOf(args[9]);
		boolean cmpr = Boolean.valueOf(args[10]);
		int ttl = Integer.valueOf(args[11]);
		PersistenceMode mode = JobUtils.readPersistence(Integer.valueOf(args[12]));
		int duration = (int) (Integer.valueOf(args[13]) * 1e3);
		int persec = Integer.valueOf(args[14]);
		String confFile = args[15];
		String hbaseMaster = args[16];
		int port = Integer.valueOf(args[17]);
		String zookeeperHost = args[18];
		String consumerKey = args[19];
		String consumerSecret = args[20];
		String accessToken = args[21];
		String accessTokenSecret = args[22];
		
		// Configure mode
		ReplicaConnection conn = mode.equals(PersistenceMode.DISK_ONLY) ?
				new ReplicaConnection(hbaseMaster, String.valueOf(port), zookeeperHost) :
				new ReplicaConnection(confFile, null, null);
		StreamingReplicaDetector detector = new StreamingReplicaDetector(mode, conn);
		
		// Create context
		SparkConf conf = new SparkConf();
		conf.set("spark.streaming.receiver.maxRate", String.valueOf(persec));
		final JavaStreamingContext spark = new JavaStreamingContext(conf, new Duration(duration));
		
		// Initialization from parameters
		try {
			detector.initialize(conn, descParams, filtParams, numTables, W, 
					hammingThresh, blockEncd, cmpr, ttl);
		} catch (InvalidArgumentException | InitializationException e) {
			System.out.println("System initialization error: " + e.getMessage());
			System.exit(1);
		}
		
		// Schedule streaming
		detector.indexStreaming(spark, consumerKey, consumerSecret, 
				accessToken, accessTokenSecret);

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
