package org.twitterReplica.jobs;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.opencv.core.Core;
import org.twitterReplica.core.ReplicaConnection;
import org.twitterReplica.core.batch.BatchReplicaDetector;
import org.twitterReplica.exceptions.ConnectionException;
import org.twitterReplica.exceptions.DataException;
import org.twitterReplica.exceptions.QueryException;
import org.twitterReplica.model.ImageInfo;
import org.twitterReplica.model.ImageMatch;
import org.twitterReplica.model.PersistenceMode;
import org.twitterReplica.utils.ReplicaUtils;

import scala.Tuple2;

public class QueryImageJob {

	public static void main(String[] args) {
		
		// Load OpenCV
		System.loadLibrary( Core.NATIVE_LIBRARY_NAME );
		
		final JavaSparkContext spark = new JavaSparkContext();
		
		// Input
		String path = args[0];
		int rank = Integer.valueOf(args[1]);
		PersistenceMode mode = JobUtils.readPersistence(Integer.valueOf(args[2]));
		String confFile= args[3];
		String hbaseMaster = args[4];
		int port = Integer.valueOf(args[5]);
		String zookeeperHost = args[6];
		
		// Configure mode
		ReplicaConnection conn = null;
		if (mode.equals(PersistenceMode.DISK_ONLY)) {
			conn = new ReplicaConnection(hbaseMaster, String.valueOf(port), zookeeperHost);
		}
		else {
			conn = new ReplicaConnection(confFile, null, null);
		}
		
		BatchReplicaDetector detector = new BatchReplicaDetector(mode, conn);
		
		// System initialization
		try {
			detector.connect(conn, spark);
		} catch (ConnectionException e) {
			System.out.println("Initialization error: " + e.getMessage());
		}
		
		System.out.println("Querying image '" + path + "' ...");
		
		JavaPairRDD<ImageMatch, Long> matchesRDD;
		try {
			
			long start = ReplicaUtils.getCPUTimeNow();
			matchesRDD = detector.queryImage(spark, new ImageInfo(path), rank);
			List<Tuple2<ImageMatch, Long>> matches = matchesRDD.collect();
			long end = ReplicaUtils.getCPUTimeNow();
			long dif = (end - start) / (long)(1e6);
			System.out.println("Elapsed: " + dif);
			if (matches.isEmpty()) {
				System.out.println("No replicas found");
			}
			else {
				System.out.println("Results:");

				// Retrieve info of the match
				for (Tuple2<ImageMatch, Long> m : matches) {
					ImageInfo img;
					try {
						System.out.println("Match 1:");
						img = detector.getImage(m._1.getMatchedId());
						System.out.println(".... Id: " + img.getId());
						System.out.println(".... Path: " + img.getPath());
						System.out.println(".... Resource id: " + img.getResourceId());
						System.out.println(".... Source: " + img.getProvider().toString());
						System.out.println(".... Weight: " + m._2);
					} catch (DataException e) {
						System.out.println("Could not retrieve information for match with image " + m._1);
					}
				}
			}
		} catch (QueryException e) {
			System.out.println(e.getMessage());
		}
		
	}
	
}
