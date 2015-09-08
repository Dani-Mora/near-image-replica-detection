package org.twitterReplica.jobs;

import java.io.File;
import java.io.IOException;

import org.apache.spark.api.java.JavaSparkContext;
import org.opencv.core.Core;
import org.twitterReplica.core.DescriptorParams;
import org.twitterReplica.core.FilteringParams;
import org.twitterReplica.core.IndexingParams;
import org.twitterReplica.core.ReplicaConnection;
import org.twitterReplica.core.ReplicaSystem;
import org.twitterReplica.core.batch.BatchReplicaDetector;
import org.twitterReplica.evaluation.ReplicaEvaluator;
import org.twitterReplica.exceptions.ConnectionException;
import org.twitterReplica.exceptions.IndexingException;
import org.twitterReplica.exceptions.InitializationException;
import org.twitterReplica.exceptions.InvalidArgumentException;
import org.twitterReplica.model.PersistenceMode;
import org.twitterReplica.model.providers.DatasetImgProvider;
import org.twitterReplica.model.providers.ProviderDisconnectedListener;
import org.twitterReplica.utils.ReplicaUtils;

/*
 * 	Evaluates the performance of the detector on the UPCReplica dataset
 * 	and saves results by image transformation into a csv
 */

public class EvaluationJob {

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
		int ranking = Integer.valueOf(args[13]);
		String datasetSrc = args[14];
		String baseImgs = args[15];
		String selected = args[16];
		String destFolder = args[17];
		String confFile = args[18];
		String hbaseMaster = args[19];
		int port = Integer.valueOf(args[20]);
		String zookeeperHost = args[21];
		int minP = Integer.valueOf(args[22]);
		
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
			detector.initialize(conn, descParams, filtParams, numTables, W, 
					hammingThresh, blockEncd, cmpr, ttl);
		} catch (InitializationException e) {
			System.out.println("System initialization error: " + e.getMessage());
			System.exit(1);
		} catch (InvalidArgumentException e) {
			System.out.println("System initialization error: " + e.getMessage());
			System.exit(1);
		}
		
		selected = baseImgs + File.separatorChar + "query.csv";
		baseImgs = baseImgs + File.separatorChar + "base.csv";
		
		// Index base images
		long initLapse = -1;
		try {
			long start = ReplicaUtils.getCPUTimeNow();
			BatchReplicaDetector det = (BatchReplicaDetector) detector;
			det.indexFromDataset(spark, baseImgs, datasetSrc, true, minP);
			long end = ReplicaUtils.getCPUTimeNow();
			initLapse = (long) ((end - start));
		} catch (IndexingException e) {
			System.out.println("Error indexing images: " + e.getMessage());
			System.exit(1);
		}
		
		// Get all indexing parameters from detector
		IndexingParams indParams = detector.getIndexingParams();
		
		// Initialize provider
		DatasetImgProvider provider = new DatasetImgProvider(datasetSrc, selected, ',');
		// For every image received found queries and check results
		final ReplicaEvaluator evaluator = new ReplicaEvaluator(detector, spark, destFolder, initLapse, 
				descParams, filtParams, indParams, ranking, minP);
		try {
			evaluator.initialize();
		} catch (IOException e) {
			System.out.println("Unexpected IO error: " + e.getMessage());
			System.exit(1);
		}
		
		provider.registerReceivedListener(evaluator);
		// when ended, disconnect event is triggered
		provider.registerDisconnectedListener(new ProviderDisconnectedListener() {
			public void onDisconnection() {
				evaluator.finalize();
			}
		});
		
		try {
			provider.connect();
		} catch (ConnectionException e1) {
			System.out.println("Error connecting: " + e1.getMessage());
		} catch (InitializationException e1) {
			System.out.println("Could not initialize provider: " + e1.getMessage());
		}
	}
	
}
