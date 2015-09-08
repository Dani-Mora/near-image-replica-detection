package org.twitterReplica.jobs;

import org.apache.spark.api.java.JavaSparkContext;
import org.opencv.core.Core;
import org.twitterReplica.core.ReplicaConnection;
import org.twitterReplica.core.ReplicaSystem;
import org.twitterReplica.core.batch.BatchReplicaDetector;
import org.twitterReplica.exceptions.ConnectionException;
import org.twitterReplica.exceptions.IndexingException;
import org.twitterReplica.model.PersistenceMode;

/*
 *  Indexes new images from the UPCReplica dataset
 */

public class IndexingJob {

	public static void main(String[] args) {
		
		// Load OpenCV
		System.loadLibrary( Core.NATIVE_LIBRARY_NAME );
		
		final JavaSparkContext spark = new JavaSparkContext();
		
		PersistenceMode mode = JobUtils.readPersistence(Integer.valueOf(args[0]));
		String datasetSrc = args[1];
		String baseImgs = args[2];
		boolean reset = Boolean.valueOf(args[3]);
		String confFile = args[4];
		String hbaseMaster = args[5];
		int port = Integer.valueOf(args[6]);
		String zookeeperHost = args[7];
		int minP = Integer.valueOf(args[8]);
		
		// Configure mode
		ReplicaConnection conn = null;
		if (mode.equals(PersistenceMode.DISK_ONLY)) {
			conn = new ReplicaConnection(hbaseMaster, String.valueOf(port), zookeeperHost);
		}
		else {
			conn = new ReplicaConnection(confFile, null, null);
		}
		
		ReplicaSystem detector = new BatchReplicaDetector(mode, conn);
		
		// Initialization from parameters
		try {
			detector.connect(conn, spark);
		} catch (ConnectionException e) {
			System.out.println("System initialization error: " + e.getMessage());
			System.exit(1);
		}
		
		// Index base images
		try {
			BatchReplicaDetector det = (BatchReplicaDetector) detector;
			det.indexFromDataset(spark, baseImgs, datasetSrc, reset, minP);
		} catch (IndexingException e) {
			System.out.println("Error indexing images: " + e.getMessage());
			System.exit(1);
		}
		
	}

}
