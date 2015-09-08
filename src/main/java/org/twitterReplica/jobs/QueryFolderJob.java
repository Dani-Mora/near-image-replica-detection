package org.twitterReplica.jobs;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.opencv.core.Core;
import org.twitterReplica.core.ReplicaConnection;
import org.twitterReplica.core.batch.BatchReplicaDetector;
import org.twitterReplica.exceptions.ConnectionException;
import org.twitterReplica.exceptions.QueryException;
import org.twitterReplica.model.ImageMatch;
import org.twitterReplica.model.PersistenceMode;
import org.twitterReplica.utils.ReplicaUtils;

import scala.Tuple2;

public class QueryFolderJob {

	static Logger logger = Logger.getLogger(QueryFolderJob.class);
	
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
		int minP = Integer.valueOf(args[7]);
		
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
		
		try {
			long start = ReplicaUtils.getCPUTimeNow();
			JavaPairRDD<ImageMatch, Long> matches = detector.queryFromFolder(spark, path, rank, minP);
			List<Tuple2<ImageMatch, Long>> matchesArray = matches.collect();
			long end = ReplicaUtils.getCPUTimeNow();
			long dif = (long) ((end - start) / 1e9);
			System.out.println("Total: " + dif + " seconds");
			
			// Show matches
			for (Tuple2<ImageMatch, Long> match : matchesArray) {
				System.out.println(" --- Found match " + match._1.getQueryId() + " with " 
						+ match._1.getMatchedId() + ", weight " + match._2);
			}
			
		} catch (QueryException e) {
			System.out.println(e.getMessage());
		}
		
	}
	
}
