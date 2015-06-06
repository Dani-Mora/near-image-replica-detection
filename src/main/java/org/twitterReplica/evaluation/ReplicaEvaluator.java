package org.twitterReplica.evaluation;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.twitterReplica.core.DescriptorParams;
import org.twitterReplica.core.FilteringParams;
import org.twitterReplica.core.IndexingParams;
import org.twitterReplica.core.ReplicaSystem;
import org.twitterReplica.core.batch.BatchReplicaDetector;
import org.twitterReplica.exceptions.QueryException;
import org.twitterReplica.model.ImageInfo;
import org.twitterReplica.model.ImageMatch;
import org.twitterReplica.model.providers.ImageResourceListener;
import org.twitterReplica.model.replica.ReplicaType;
import org.twitterReplica.utils.ReplicaUtils;

import au.com.bytecode.opencsv.CSVWriter;

import scala.Tuple2;

public class ReplicaEvaluator implements ImageResourceListener {

	static Logger logger = Logger.getLogger(ReplicaEvaluator.class);
	
	private JavaSparkContext spark;
	
	// Overall performance
	private Performance perf;
	private int replicas;
	private int backgrounds;
	private long totalQueryTime;
	private long initLapseTime;
	
	private Integer rankingThreshold;
	
	// Writing
	private String statsFolder;
	private CSVWriter writer;
	
	// Input parameters
	private DescriptorParams descParams;
	private FilteringParams filtParams; 
	private IndexingParams indParams;
	
	// Lists of weights
	private List<Long> truePositiveWeights;
	private List<Long> falsePositiveWeights;
	
	private ReplicaSystem detector;
	
	// CSV Header
	private static final String[] HEADER = new String[] { "Descriptor_type",
		"Keypoint_detector", "Max_length_side", "Filtering_type", "Filtering_thresh", "Log_scale", "Num_tables",
		"W", "Hamming_distance", "Indexing_time", "Query_average_time", "Total_precision", "Total_recall",
		"Channel_S_precision", "Channel_S_recall", "Channel_V_precision", "Channel_V_recall",
		"Compressed_precision", "Compressed_recall", "Gamma_precision", "Gamma_recall", 
		"Gaussian_precision", "Gaussian_recall", "Occluded_precision", "Occluded recall",
		"HFlipped_precision", "HFlipped_recall", "Resized_precision", "Resized_recall",
		"Rotated_precision", "Rotated_recall", "Cropped_H_precision", "Cropped_H_recall",
		"Cropped_V_precision", "Cropped_V_recall", "Smoothed_precision", "Smoothed_recall",
		"Text_precision", "Text_recall", "True_positive_rate", "False_positive_rate", "Ranking" };
	
	// Replica type performances
	Map<ReplicaType, Performance> replicaStats;
	
	/*
	 * 	Initializes the performance evaluator of the replica detector system
	 * 	@param spark Java spark context
	 * 	@param statsFolder Destination folder of the stats of the test
	 * 	@param initLapse Time elapsed during system initialization
	 * 	@param descParams Descriptor computation parameters
	 * 	@param filtParams Filtering parameters for features
	 * 	@param indParams Indexing parameters
	 */
	public ReplicaEvaluator(ReplicaSystem system, JavaSparkContext spark, String statsFolder, long initLapse, 
			DescriptorParams descParams, FilteringParams filtParams, IndexingParams indParams, int rankThresh) {
		this.detector = system;
		this.spark = spark;
		this.replicas = 0;
		this.backgrounds = 0;
		this.totalQueryTime = 0L;
		this.initLapseTime = initLapse;
		this.descParams = descParams;
		this.filtParams = filtParams;
		this.indParams = indParams;
		this.perf = new Performance();
		this.replicaStats = new HashMap<ReplicaType, Performance>();
		this.statsFolder = statsFolder;
		this.rankingThreshold = rankThresh;
		this.truePositiveWeights = new ArrayList<Long>();
		this.falsePositiveWeights = new ArrayList<Long>();
	}
	
	public void imageResReceived(ImageInfo r) {
		
		BatchReplicaDetector det = (BatchReplicaDetector) detector;
		JavaPairRDD<ImageMatch, Long> matches;
		try {
			// Image received
			long start = ReplicaUtils.getCPUTimeNow();
			matches = det.queryImage(spark, r, this.rankingThreshold);
			List<Tuple2<ImageMatch, Long>> matchesArray = matches.toArray();
			long end = ReplicaUtils.getCPUTimeNow();
			long lapse = (long) (end - start);
			logger.warn("Image " + r.getId() + ".Lasted: " + lapse / 1e9);
			this.totalQueryTime += lapse; 
			
			// Results to be interpreted differently depending on replica or not
			if (r.isReplica()) {
				
				this.replicas++;
				
				boolean found = false;
				for (Tuple2<ImageMatch, Long> match : matchesArray) {
					if (match._1.getMatchedId() == r.getSrcId()) {
						this.perf.addTruePositive();
						logger.warn("---- Found replica with weight " + match._2());
						addReplicaTruePositive(r.getReplicaType());
						found = true;
						
						// Add weight to list
						this.truePositiveWeights.add(match._2());
					}
					else {
						this.perf.addFalsePositive();
						logger.warn("---- Found false positive " + match._1() + " with weight " + match._2());
						addReplicaFalsePositive(r.getReplicaType());
						
						// Add weight to list
						this.falsePositiveWeights.add(match._2());
					}
				}
				
				// If replica was not found, then add a false negative
				if (!found) {
					this.perf.addFalseNegative();
					logger.warn("---- Replica not found");
					addReplicaFalseNegative(r.getReplicaType());
				}
			}
			else {
				
				this.backgrounds++;
				
				if (matchesArray.isEmpty()) {
					this.perf.addTrueNegative();
				}
				
				for (Tuple2<ImageMatch, Long> match : matchesArray) {
					System.out.println("---- Found false positive " + match._1() + " with weight " + match._2());
					this.perf.addFalsePositive();
					
					// Add weight to list
					this.falsePositiveWeights.add(match._2());
				}
			}
			
		} catch (QueryException e) {
			logger.error("Error querying file: " + e.getMessage());
		}
		
	}
	
	public void initialize() throws IOException {
		// Init writer with file name
		String fileName = new SimpleDateFormat("yyyyMMddhhmm'.txt'").format(new Date());
		String path =  this.statsFolder + File.separatorChar + fileName;
		this.writer = new CSVWriter(new FileWriter(path));
	}

	public void finalize() {
		
		// Write header
		this.writer.writeNext(HEADER);
		
		long queryAvg = this.totalQueryTime / (long) (this.backgrounds + this.replicas);
		double tpr = this.replicas == 0 ? 0 :(double)  this.perf.getTruePositives() / (double) this.replicas;
		double fpr = this.backgrounds == 0 ? 0 : (double)  this.perf.getFalsePositives() / (double) this.backgrounds;
		
		// Prepare data
		List<String[]> list = new LinkedList<String[]>();
		String[] line = new String[HEADER.length];
		line[0] = (this.descParams.getDescriptorType().toString());
		line[1] = (this.descParams.getKeypointType().toString());
		line[2] = (String.valueOf(this.descParams.getMaximumLargestSide()));
		line[3] = (this.filtParams.getFilteringType().toString());
		line[4] = (String.valueOf(this.filtParams.getThresh()));
		line[5] = (String.valueOf(this.filtParams.isLogScaleEnabled()));
		line[6] = (String.valueOf(this.indParams.getNumTables()));
		line[7] = (String.valueOf(this.indParams.getSketchFunction().getW()));
		line[8] = (String.valueOf(this.indParams.getHammingDistance()));
		line[9] = (String.valueOf(this.initLapseTime));
		line[10] = (String.valueOf(queryAvg));
		line[11] = (String.valueOf(this.perf.getPrecision()));
		line[12] = (String.valueOf(this.perf.getRecall()));
		line[13] = (String.valueOf(this.replicaStats.get(ReplicaType.ChannelS).getPrecision()));
		line[14] = (String.valueOf(this.replicaStats.get(ReplicaType.ChannelS).getRecall()));
		line[15] = (String.valueOf(this.replicaStats.get(ReplicaType.ChannelV).getPrecision()));
		line[16] = (String.valueOf(this.replicaStats.get(ReplicaType.ChannelV).getRecall()));
		line[17] = (String.valueOf(this.replicaStats.get(ReplicaType.Compressed).getPrecision()));
		line[18] = (String.valueOf(this.replicaStats.get(ReplicaType.Compressed).getRecall()));
		line[19] = (String.valueOf(this.replicaStats.get(ReplicaType.Gamma).getPrecision()));
		line[20] = (String.valueOf(this.replicaStats.get(ReplicaType.Gamma).getRecall()));
		line[21] = (String.valueOf(this.replicaStats.get(ReplicaType.Gaussian).getPrecision()));
		line[22] = (String.valueOf(this.replicaStats.get(ReplicaType.Gaussian).getRecall()));
		line[23] = (String.valueOf(this.replicaStats.get(ReplicaType.Occluded).getPrecision()));
		line[24] = (String.valueOf(this.replicaStats.get(ReplicaType.Occluded).getRecall()));
		line[25] = (String.valueOf(this.replicaStats.get(ReplicaType.HFlipped).getPrecision()));
		line[26] = (String.valueOf(this.replicaStats.get(ReplicaType.HFlipped).getRecall()));
		line[27] = (String.valueOf(this.replicaStats.get(ReplicaType.Resized).getPrecision()));
		line[28] = (String.valueOf(this.replicaStats.get(ReplicaType.Resized).getRecall()));
		line[29] = (String.valueOf(this.replicaStats.get(ReplicaType.Rotated).getPrecision()));
		line[30] = (String.valueOf(this.replicaStats.get(ReplicaType.Rotated).getRecall()));
		line[31] = (String.valueOf(this.replicaStats.get(ReplicaType.Cropped_H).getPrecision()));
		line[32] = (String.valueOf(this.replicaStats.get(ReplicaType.Cropped_H).getRecall()));
		line[33] = (String.valueOf(this.replicaStats.get(ReplicaType.Cropped_V).getPrecision()));
		line[34] = (String.valueOf(this.replicaStats.get(ReplicaType.Cropped_V).getRecall()));
		line[35] = (String.valueOf(this.replicaStats.get(ReplicaType.Smoothed).getPrecision()));
		line[36] = (String.valueOf(this.replicaStats.get(ReplicaType.Smoothed).getRecall()));
		line[37] = (String.valueOf(this.replicaStats.get(ReplicaType.Text).getPrecision()));
		line[38] = (String.valueOf(this.replicaStats.get(ReplicaType.Text).getRecall()));
		line[39] = (String.valueOf(tpr));
		line[40] = (String.valueOf(fpr));
		line[41] = (String.valueOf(this.rankingThreshold));
		list.add(line);
		this.writer.writeAll(list);
		
		// Close writer
		try {
			this.writer.close();
		} catch (IOException e) {
			logger.warn("Error closing writer: " + e.getMessage());
		}
		
		try {
			dumpIntoFile(this.statsFolder + File.separatorChar + "true_positives.txt", this.truePositiveWeights);
			dumpIntoFile(this.statsFolder + File.separatorChar + "false_positives.txt", this.falsePositiveWeights);
		} catch (IOException e) {
			System.out.println("Error dumping weights: " + e.getMessage());
		}
		
	}
	
	private void addReplicaFalsePositive(ReplicaType label) {
		if (!this.replicaStats.containsKey(label)) {
			this.replicaStats.put(label, new Performance());
		}
		this.replicaStats.get(label).addFalsePositive();
	}
	
	private void addReplicaTruePositive(ReplicaType label) {
		if (!this.replicaStats.containsKey(label)) {
			this.replicaStats.put(label, new Performance());
		}
		this.replicaStats.get(label).addTruePositive();
	}
	
	private void addReplicaFalseNegative(ReplicaType label) {
		if (!this.replicaStats.containsKey(label)) {
			this.replicaStats.put(label, new Performance());
		}
		this.replicaStats.get(label).addFalseNegative();
	}
	
	private void dumpIntoFile(String file, List<Long> list) throws IOException {
		FileWriter fWriter = new FileWriter(file);
		boolean first = true;
		for (Long i : list) {
			if (first) {
				first = false;
			}
			else {
				fWriter.write(",");
			}
			fWriter.write(String.valueOf(i));
		}
		fWriter.close();
	}
}
