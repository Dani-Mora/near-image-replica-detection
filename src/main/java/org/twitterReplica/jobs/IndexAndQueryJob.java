package org.twitterReplica.jobs;

import java.io.File;
import java.util.List;

import org.apache.log4j.Logger;
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
import org.twitterReplica.model.ImageInfo;
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

		final JavaSparkContext spark = new JavaSparkContext();
		
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
			det.indexFromFolder(spark, folder, false);
		} catch (IndexingException e) {
			System.out.println("Error indexing images: " + e.getMessage());
			System.exit(1);
		}
		
		// Query folder
		File queryFolder = new File(queryPath);
		double total = 0.0;
		double count = 0.0;
		File[] files = queryFolder.listFiles();
		
		for (File f : files) {
			
			JavaPairRDD<ImageMatch, Long> matchesRDD;
			try {
				long start = ReplicaUtils.getCPUTimeNow();
				matchesRDD = detector.queryImage(spark, new ImageInfo(f.getAbsolutePath()), rank);
				List<Tuple2<ImageMatch, Long>> matchesArray = matchesRDD.toArray();
				long end = ReplicaUtils.getCPUTimeNow();
				long lapse = (long) ((end - start));
				total += lapse;
				
				// Show matches
				for (Tuple2<ImageMatch, Long> match : matchesArray) {
					logger.warn(" --- Found match " + match._1.getMatchedId() + " with weight " + match._2);
				}
				
				// Increase count 
				count = count + 1;
			} catch (QueryException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		System.out.println("Count: " + count);
		double average = total / count;
		System.out.println("Average: " + average);
	}

}
