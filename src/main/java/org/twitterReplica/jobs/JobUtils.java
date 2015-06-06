package org.twitterReplica.jobs;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.twitterReplica.core.DescriptorParams;
import org.twitterReplica.core.FilteringParams;
import org.twitterReplica.exceptions.InvalidArgumentException;
import org.twitterReplica.model.DescriptorType;
import org.twitterReplica.model.FilteringType;
import org.twitterReplica.model.keypoints.KeypDetectors;
import org.twitterReplica.model.PersistenceMode;


public class JobUtils {

	static Logger logger = Logger.getLogger(JobUtils.class);
	
	public static final String APP_NAME = "Replica_Detector";
	
	private static final int SIFT_DESC = 0;
	private static final int SURF_DESC = 1;
	private static final int ORB_DESC = 2;
	private static final int FREAK_DESC = 3;
	private static final int BRISK_DESC = 4;
	private static final int BRIEF_DESC = 5;
	
	private static final int SIFT_KEYP = 0;
	private static final int SURF_KEYP = 1;
	private static final int ORB_KEYP = 2;
	private static final int MSER_KEYP = 3;
	private static final int BRISK_KEYP = 4;
	private static final int HARRIS_KEYP = 5;
	
	private static final int FILTERING_NONE = 0;
	private static final int FILTERING_ENTROPY = 1;
	private static final int FILTERING_VARIANCE = 2;
	
	private static final int PERSISTENCE_MEMORY_ONLY = 0;
	private static final int PERSISTENCE_DISK_ONLY = 1;
	
	public static JavaSparkContext getSparkContext(int cores) {
		final JavaSparkContext spark;
		String master = "local[" + String.valueOf(cores) + "]";
		spark = new JavaSparkContext(master, APP_NAME);
		return spark;
	}
	
	public static JavaStreamingContext getStreamingContext(int cores, int duration, String[] libs, int persec) {
		String master = "local[" + String.valueOf(cores) + "]";
		SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster(master);
		conf.set("spark.streaming.receiver.maxRate", String.valueOf(persec));
		conf.setJars(libs);
		JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(duration));
		return ssc;
	}
	
	/*
	 * 	Descriptor parameters
	 */
	
	public static DescriptorParams readDescParamsFromInput(String args[])
			throws InvalidArgumentException{
		
		DescriptorType descType = null;
		KeypDetectors keypType = null;
		
		// Descriptor param
		int descInteger = Integer.valueOf(args[0]);
		switch(descInteger) {
			case SIFT_DESC:
				descType = DescriptorType.SIFT;
				break;
			case SURF_DESC:
				descType = DescriptorType.SURF;
				break;
			case ORB_DESC:
				descType = DescriptorType.ORB;
				break;
			case FREAK_DESC:
				descType = DescriptorType.FREAK;
				break;
			case BRISK_DESC:
				descType = DescriptorType.BRISK;
				break;
			case BRIEF_DESC:
				descType = DescriptorType.BRIEF;
				break;
			default:
				throw new InvalidArgumentException("Unknown descriptor: " + descInteger);
		}
		
		// Keypoint detection param
		int keypInteger = Integer.valueOf(args[1]);
		switch(keypInteger) {
			case SIFT_KEYP:
				keypType = KeypDetectors.SIFT;
				break;
			case SURF_KEYP:
				keypType = KeypDetectors.SURF;
				break;
			case ORB_KEYP:
				keypType = KeypDetectors.ORB;
				break;
			case MSER_KEYP:
				keypType = KeypDetectors.MSER;
				break;
			case BRISK_KEYP:
				keypType = KeypDetectors.BRISK;
				break;
			case HARRIS_KEYP:
				keypType = KeypDetectors.HARRIS;
				break;
			default:
				throw new InvalidArgumentException("Unknown keypoint detector: " + keypInteger);
		}

		// Maximum side
		int maxSide = Integer.valueOf(args[2]);
				
		return new DescriptorParams(descType, keypType, maxSide);
	}
	
	public static FilteringParams readFiltParamsFromInput(String args[])
			throws InvalidArgumentException{
		
		FilteringType filteringType = null;
		
		// Descriptor param
		int filtInteger = Integer.valueOf(args[3]);
		switch(filtInteger) {
			case FILTERING_NONE:
				filteringType = FilteringType.NONE;
				break;
			case FILTERING_ENTROPY:
				filteringType = FilteringType.ENTROPY;
				break;
			case FILTERING_VARIANCE:
				filteringType = FilteringType.VARIANCE;
				break;
			default:
				throw new InvalidArgumentException("Unknown descriptor: " + filtInteger);
		}
		
		Double thresh = Double.valueOf(args[4]);
		boolean logScale = Boolean.parseBoolean(args[5]);
		
		// Maximum side
		return new FilteringParams(filteringType, thresh, logScale);
	}
	
	public static void printEvaluationUsage() {
		printIndexingResetUsage();
		System.out.println("-- 12th argument: Path to CSV containing images to query");
		System.out.println("-- 13th argument: Path to performance CSV destination file");
	}
	
	public static void printQueryImageUsage() {
		System.out.println("-- 1st argument: Image path");
	}
	
	public static void printQueryUsage() {
		System.out.println("-- 1st argument: Dataset location");
		System.out.println("-- 2nd argument: Images to index");
		System.out.println("-- 3rd argument: Destination folder");
	}
	
	public static void printIndexingUsage() {
		printIndexingBasic();
		System.out.println("-- 1st argument: Dataset path");
		System.out.println("-- 2ndth argument: Path to CSV containing images to index");
	}
	
	public static void printIndexingResetUsage() {
		printIndexingBasic();
		System.out.println("-- 10th argument: Dataset path");
		System.out.println("-- 11th argument: Path to CSV containing images to index");
	}

	public static void printScalabilityTest() {
		printIndexingBasic();
		System.out.println("-- 10th argument: Folder where images are stored");
		System.out.println("-- WARNING: Argument 9 not used in this experiment");
	}
	
	private static void printIndexingBasic() {
		System.out.println("Arguments:");
		System.out.println("-- 1st argument: Descriptor Type");
		System.out.println("------- SIFT: " + SIFT_DESC);
		System.out.println("------- SURF: " + SURF_DESC);
		System.out.println("------- ORB: " + ORB_DESC);
		System.out.println("------- FREAK: " + FREAK_DESC);
		System.out.println("------- BRISK: " + BRISK_DESC);
		System.out.println("------- BRIEF: " + BRIEF_DESC);
		System.out.println("-- 2nd argument: Keypoint detector Type");
		System.out.println("------- SIFT: " + SIFT_KEYP);
		System.out.println("------- SURF: " + SURF_KEYP);
		System.out.println("------- ORB: " + ORB_KEYP);
		System.out.println("------- MSER: " + MSER_KEYP);
		System.out.println("------- BRISK: " + BRISK_KEYP);
		System.out.println("------- HARRIS: " + HARRIS_KEYP);
		System.out.println("-- 3d argument: Side of maximum largest side when computing descriptor");
		System.out.println("-- 4th argument: Filtering type");
		System.out.println("------- NONE: " + FILTERING_NONE);
		System.out.println("------- ENTROPY: " + FILTERING_ENTROPY);
		System.out.println("------- VARIANCE: " + FILTERING_VARIANCE);
		System.out.println("-- 5th argument: Filtering threshold");
		System.out.println("-- 6th argument: Log scaling option (true/false)");
		System.out.println("-- 7th argument: Number of tables");
		System.out.println("-- 8th argument: W (Number of bins of feature sketching)");
		System.out.println("-- 9th argument: Hamming distance threshold between sketches");
	}
	
	public static PersistenceMode readPersistence(Integer arg) {
		PersistenceMode result = null;
		switch(arg) {
		case PERSISTENCE_MEMORY_ONLY:
			result = PersistenceMode.MEMORY_ONLY;
			break;
		case PERSISTENCE_DISK_ONLY:
			result = PersistenceMode.DISK_ONLY;
			break;
		default:
			result = null;
		}
		return result;
	}
	
}
