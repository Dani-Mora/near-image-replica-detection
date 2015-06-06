package org.twitterReplica.jobs;

import org.opencv.core.Core;
import org.twitterReplica.core.DescriptorParams;
import org.twitterReplica.core.FilteringParams;
import org.twitterReplica.core.ReplicaConnection;
import org.twitterReplica.core.ReplicaSystem;
import org.twitterReplica.core.batch.BatchReplicaDetector;
import org.twitterReplica.exceptions.InitializationException;
import org.twitterReplica.exceptions.InvalidArgumentException;
import org.twitterReplica.model.PersistenceMode;

/*
 * 	Indexes images given the parameters and erases all content stored so far
 */
public class ResetJob {

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
		String confFile = args[13];
		String hbaseMaster = args[14];
		int port = Integer.valueOf(args[15]);
		String zookeeperHost = args[16];
		
		// Configure mode
		ReplicaConnection conn = mode.equals(PersistenceMode.DISK_ONLY) ?
				new ReplicaConnection(hbaseMaster, String.valueOf(port), zookeeperHost) :
				new ReplicaConnection(confFile, null, null);
		
		ReplicaSystem detector = new BatchReplicaDetector(mode, conn);
		
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
		
	}

}
