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
 *  Indexes images from the input folder
 */

public class IndexFolderJob {

	public static void main(String[] args) {
		
		// Load OpenCV
		System.loadLibrary( Core.NATIVE_LIBRARY_NAME );
		
		final JavaSparkContext spark = new JavaSparkContext();
		
		PersistenceMode mode = JobUtils.readPersistence(Integer.valueOf(args[0]));
		String folder = args[1];
		boolean reset = Boolean.valueOf(args[2]);
		String confFile = args[3];
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
		
		ReplicaSystem detector = new BatchReplicaDetector(mode, conn);
		
		// Initialization from parameters
		try {
			detector.connect(conn, spark);
		} catch (ConnectionException e) {
			System.out.println("System initialization error: " + e.getMessage());
			System.exit(1);
		}
		
		try {
			BatchReplicaDetector det = (BatchReplicaDetector) detector;
			det.indexFromFolder(spark, folder, reset);
		} catch (IndexingException e) {
			System.out.println("Error indexing images: " + e.getMessage());
			System.exit(1);
		}
		
	}

}
