package org.twitterReplica.jobs;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.opencv.core.Core;
import org.twitterReplica.core.DescriptorParams;
import org.twitterReplica.core.FilteringParams;
import org.twitterReplica.core.ReplicaConnection;
import org.twitterReplica.core.batch.BatchReplicaDetector;
import org.twitterReplica.exceptions.IndexingException;
import org.twitterReplica.exceptions.InitializationException;
import org.twitterReplica.exceptions.InvalidArgumentException;
import org.twitterReplica.exceptions.QueryException;
import org.twitterReplica.model.ImageMatch;
import org.twitterReplica.model.PersistenceMode;
import org.twitterReplica.utils.ReplicaUtils;

import scala.Tuple2;

/*
 * 	Indexes and queries images from different folders
 */
public class IndexAndQueryJob {

	static Logger logger = Logger.getLogger(IndexAndQueryJob.class);
	
	public static void main(String[] args) {

		// Load OpenCV
		System.loadLibrary( Core.NATIVE_LIBRARY_NAME );

		final SparkConf config = new SparkConf();
		config.set("spark.task.maxFailures", "4");
		final JavaSparkContext spark = new JavaSparkContext(config);
		
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
		String folder = args[13];
		String confFile = args[14];
		String hbaseMaster = args[15];
		int port = Integer.valueOf(args[16]);
		String zookeeperHost = args[17];
		String queryPath = args[18];
		int rank = Integer.valueOf(args[19]);
		int minP = Integer.valueOf(args[20]);
		
		// Configure mode
		ReplicaConnection conn = null;
		if (mode.equals(PersistenceMode.DISK_ONLY)) {
			conn = new ReplicaConnection(hbaseMaster, String.valueOf(port), zookeeperHost);
		}
		else {
			conn = new ReplicaConnection(confFile, null, null);
		}
		
		BatchReplicaDetector detector = new BatchReplicaDetector(mode, conn);
		
		// Initialization from parameters
		try {
			detector.initialize(conn, descParams, filtParams, numTables, W, 
					hammingThresh, blockEncd, cmpr, ttl);
		} catch (InitializationException e) {
			System.out.println("System initialization error: " + e.getMessage());
			System.exit(1);
		} catch (InvalidArgumentException e) {
			System.out.println("System initialization error: " + e.getMessage());
			System.exit(1);
		}
		
		// Index base images
		try {
			BatchReplicaDetector det = (BatchReplicaDetector) detector;
			
			long start, end, lapse;
			
			// Index images
			start = ReplicaUtils.getCPUTimeNow();
			det.indexFromFolder(spark, folder, false, minP);
			end = ReplicaUtils.getCPUTimeNow();
			lapse = (long) (end - start);
			logger.warn("Indexing time: " + lapse + " nanoseconds");
			
			// Query folder
			start = ReplicaUtils.getCPUTimeNow();
			JavaPairRDD<ImageMatch, Long> matchesRDD = det.queryFromFolder(spark, queryPath, rank, minP);
			List<Tuple2<ImageMatch, Long>> matchesArray = matchesRDD.toArray();
			end = ReplicaUtils.getCPUTimeNow();
			lapse = (long) (end - start);
			logger.warn("Query time: " + lapse + " nanoseconds");

			// Show matches
			for (Tuple2<ImageMatch, Long> match : matchesArray) {
				logger.warn(" --- Found image " + match._1.getQueryId() + " matching " + match._1.getMatchedId() + " with weight " + match._2);
			}
			
		} catch (IndexingException e) {
			System.out.println("Error indexing folder: " + e.getMessage());
			System.exit(1);
		} catch (QueryException e) {
			System.out.println("Error querying folder: " + e.getMessage());
			System.exit(1);
		}
	
	}
		
}
