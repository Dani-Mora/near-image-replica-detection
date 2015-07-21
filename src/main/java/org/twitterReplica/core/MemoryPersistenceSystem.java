package org.twitterReplica.core;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.twitterReplica.exceptions.DataException;
import org.twitterReplica.exceptions.NotFoundException;
import org.twitterReplica.model.DescriptorType;
import org.twitterReplica.model.FilteringType;
import org.twitterReplica.model.ImageFeature;
import org.twitterReplica.model.ImageInfo;
import org.twitterReplica.model.SketchFunction;
import org.twitterReplica.model.keypoints.KeypDetectors;
import org.twitterReplica.spark.indexing.memory.FeatureToBlock;
import org.twitterReplica.spark.indexing.memory.ImagesToIDPair;
import org.twitterReplica.spark.input.PropertiesReader;
import org.twitterReplica.spark.query.memory.FilterTableMatches;
import org.twitterReplica.spark.query.memory.GroupTableMatches;
import org.twitterReplica.spark.query.memory.MapToFeatureMatch;
import org.twitterReplica.spark.query.memory.WeightTableMatches;
import org.twitterReplica.utils.ReplicaUtils;

import scala.Tuple2;

public class MemoryPersistenceSystem implements IPersistence {

	static Logger logger = Logger.getLogger(MemoryPersistenceSystem.class);
	
	private boolean empty = true;
	private List<JavaPairRDD<Integer, ImageFeature>> tables = new LinkedList<JavaPairRDD<Integer, ImageFeature>>();
	private JavaPairRDD<Long, ImageInfo> images;
	
	private static StorageLevel MEMORY_MODE = StorageLevel.MEMORY_ONLY();
	
	private static final String PROPS_SEPARATOR = "_";
	
	private static final String LINE_SPACE = "\n";
	
	private static final String DESC_TYPE_KEY = "descT";
	private static final String KEYP_KEY = "keyP";
	private static final String SIDE_KEY = "side";
	
	private static final String NUM_TABLES_KEY = "tables";
	private static final String HAMMING_KEY = "hamming";
	private static final String W_KEY = "W";
	private static final String A_KEY = "A";
	private static final String B_KEY = "B";
	
	private static final String FILT_KEY = "fType";
	private static final String THRESH_KEY = "thresh";
	private static final String LOG_SCALE = "logscale";
	
	@Override
	public DescriptorParams readDescriptorParams(ReplicaConnection params, JavaSparkContext spark)
			throws NotFoundException, DataException {
		JavaPairRDD<String, String> props = readPropertiesFile(spark, params.getFirstParam());
		// Read properties from file
		DescriptorType descType = DescriptorType.valueOf(props.lookup(DESC_TYPE_KEY).get(0));
		KeypDetectors keypType = KeypDetectors.valueOf(props.lookup(KEYP_KEY).get(0));
		int side = Integer.valueOf(props.lookup(SIDE_KEY).get(0));
		return new DescriptorParams(descType, keypType, side);
	}

	@Override
	public IndexingParams readIndexingParams(ReplicaConnection params, 
			JavaSparkContext spark, int featureSize) throws NotFoundException, DataException {
		JavaPairRDD<String, String> props = readPropertiesFile(spark, params.getFirstParam());
		// Read properties from file
		int numTables = Integer.valueOf(props.lookup(NUM_TABLES_KEY).get(0));
		int hamming = Integer.valueOf(props.lookup(HAMMING_KEY).get(0));
		int W = Integer.valueOf(props.lookup(W_KEY).get(0));
		double b = Double.valueOf(props.lookup(B_KEY).get(0));
		double[] a = ReplicaUtils.getArrayFromString(props.lookup(A_KEY).get(0));
		SketchFunction func = new SketchFunction(a, b, W);
		return new IndexingParams(func, numTables, hamming, false, false, -1);
	}

	@Override
	public FilteringParams readFilteringParams(ReplicaConnection conn, JavaSparkContext spark) {
		JavaPairRDD<String, String> props = readPropertiesFile(spark, conn.getFirstParam());
		// Read properties from file
		FilteringType fType = FilteringType.valueOf(props.lookup(FILT_KEY).get(0));
		double thresh = Double.valueOf(props.lookup(THRESH_KEY).get(0));
		boolean log = Boolean.valueOf(props.lookup(LOG_SCALE).get(0));
		return new FilteringParams(fType, thresh, log);
	}

	/*
	 * 	Reads properties from the input path as key-value
	 * 	@param spark Spark context
	 * 	@param path Path to the properties file
	 */
	protected static JavaPairRDD<String, String> readPropertiesFile(JavaSparkContext spark, String path) {
		JavaRDD<String> lines = spark.textFile(path);
		return lines.mapToPair(new PropertiesReader(PROPS_SEPARATOR));
	}
	
	@Override
	public void storeParameters(ReplicaConnection conn, DescriptorParams dParams, 
			IndexingParams iParams, FilteringParams fParams) throws DataException {
		
		try {
			// Write to file (raw or hdfs)
			Configuration conf = new Configuration();
			Path path = new Path(conn.getFirstParam());
		    FileSystem fs = path.getFileSystem(conf);
		    FSDataOutputStream out = fs.create(path, true);
	
		    // Descriptor params
		    out.writeBytes(DESC_TYPE_KEY + PROPS_SEPARATOR + dParams.getDescriptorType().toString() + LINE_SPACE);
		    out.writeBytes(KEYP_KEY + PROPS_SEPARATOR + dParams.getKeypointType().toString() + LINE_SPACE);
		    out.writeBytes(SIDE_KEY + PROPS_SEPARATOR + String.valueOf(dParams.getMaximumLargestSide()) + LINE_SPACE);
	
		    // Indexing params
		    out.writeBytes(NUM_TABLES_KEY + PROPS_SEPARATOR + String.valueOf(iParams.getNumTables()) + LINE_SPACE);
		    out.writeBytes(HAMMING_KEY + PROPS_SEPARATOR + String.valueOf(iParams.getHammingDistance()) + LINE_SPACE);
		    out.writeBytes(W_KEY + PROPS_SEPARATOR + String.valueOf(iParams.getSketchFunction().getW()) + LINE_SPACE);
		    out.writeBytes(B_KEY + PROPS_SEPARATOR + String.valueOf(iParams.getSketchFunction().getB()) + LINE_SPACE);
		    out.writeBytes(A_KEY + PROPS_SEPARATOR + ReplicaUtils.listToString(iParams.getSketchFunction().getA()) + LINE_SPACE);
		    
		    // Filtering params
		    out.writeBytes(FILT_KEY + PROPS_SEPARATOR + fParams.getFilteringType().toString() + LINE_SPACE);
		    out.writeBytes(THRESH_KEY + PROPS_SEPARATOR + String.valueOf(fParams.getThresh()) + LINE_SPACE);
		    out.writeBytes(LOG_SCALE + PROPS_SEPARATOR + fParams.isLogScaleEnabled() + LINE_SPACE);
		    
		    out.close();
		    
		} catch (IOException e) {
			throw new DataException("Could not store parameters: " + e.getMessage());
		}
		
	}

	@Override
	public void onConnected() {
		// Nothing to do
		// TODO: maybe read data from disk, future work
	}

	@Override
	public void indexData(ReplicaConnection conn,  IndexingParams ind, JavaPairRDD<ImageInfo, ImageFeature> sketches) {
		
		int numTables = ind.getNumTables();
		
		// Index sketches by block key
		List<JavaPairRDD<Integer, ImageFeature>> data = new LinkedList<JavaPairRDD<Integer, ImageFeature>>();
		for (int i = 0; i < numTables; ++i) {
			JavaPairRDD<Integer, ImageFeature> table = sketches.mapToPair(new FeatureToBlock(i));
			// Insert into data
			data.add(table);
		}
		
		// Save image metadata
		JavaRDD<ImageInfo> images = sketches.keys().distinct();
		JavaPairRDD<Long, ImageInfo> imgPair = images.mapToPair(new ImagesToIDPair());
		
		// Save
		if (this.empty) {
			this.empty = false;
			// Insert for first time
			this.images = imgPair;
			this.tables = data;
		}
		else {
			// Append to existing data
			for (int i = 0; i < data.size(); ++i) {
				this.tables.set(i, this.tables.get(i).union(data.get(i)));
			}
			this.images = this.images.union(imgPair);
		}
		
		// Save into memory
		persistData();
	}
	
	@Override
	public JavaPairRDD<ImageFeature, ImageFeature> queryFeatures(
			ReplicaConnection conn, IndexingParams params, JavaPairRDD<ImageInfo, ImageFeature> sketches) {
		
		final Integer hammingThresh = params.getHammingDistance();
		final Integer minimumTables = params.getNumTables() - hammingThresh;
		
		JavaPairRDD<Integer, Tuple2<ImageFeature, ImageFeature>> total = null;
		
		// Query each table
		for (int i = 0; i < this.tables.size(); ++i) {
			JavaPairRDD<Integer, ImageFeature> table = this.tables.get(i);
			JavaPairRDD<Integer, ImageFeature> blocks = sketches.mapToPair(new FeatureToBlock(i));
			JavaPairRDD<Integer, Tuple2<ImageFeature, ImageFeature>> matchesTable = 
					blocks.join(table);
			// Append to total
			if (total == null) {
				total = matchesTable;
			}
			else {
				total = total.union(matchesTable);
			}
		}
		
		// We have all table matches 
		JavaPairRDD<Tuple2<ImageFeature, ImageFeature>, Integer> tablesMatched = 
				total.mapToPair(new WeightTableMatches());
	
		// Group by match 
		JavaPairRDD<Tuple2<ImageFeature, ImageFeature>, Integer> tablesMatchedNum = 
				tablesMatched.reduceByKey(new GroupTableMatches());
		
		// Filter by table matches
		JavaPairRDD<Tuple2<ImageFeature, ImageFeature>, Integer> matches = 
				tablesMatchedNum.filter(new FilterTableMatches(minimumTables));
		
		return matches.mapToPair(new MapToFeatureMatch());
	}

	@Override
	public void restartData(ReplicaConnection params, IndexingParams ind) throws DataException {
		if (!this.empty) {
			
			// Unersist hash tables
			for (JavaPairRDD<Integer, ImageFeature> table : tables) {
				table.unpersist();
			}
			// Unersist image info
			this.images.unpersist();
			
			// Set to empty and null
			tables = new LinkedList<JavaPairRDD<Integer, ImageFeature>>();
			images = null;
			empty = true;
		}
	}
	
	@Override
	public void restart(ReplicaConnection params, IndexingParams ind)
			throws DataException {
		restartData(params, ind);
	}
	
	/*
	 * 	Loads indexed data into memory
	 */
	protected void persistData() {
		// Persist sketch info
		for (JavaPairRDD<Integer, ImageFeature> table : tables) {
			table.persist(MEMORY_MODE);
			// Apply action to load them in memory
			table.count();
		}
		// Persist image info
		this.images.persist(MEMORY_MODE);
		long c =this.images.count();
		System.out.println("Number of indexed images: " + c);
	}
	
	/*
	 * 	Frees the indexed resources from memory
	 */
	public void releaseMemory() {
		// Persist sketch info
		for (JavaPairRDD<Integer, ImageFeature> table : tables) {
			table.unpersist();
		}
		// Persist image info
		this.images.unpersist();
	}

	@Override
	public void indexStreaming(final ReplicaConnection conn,final IndexingParams ind,
			JavaPairDStream<ImageInfo, ImageFeature> sketches) {
		// Call index for each RDD in the Stream
		sketches.foreachRDD(new Function<JavaPairRDD<ImageInfo, ImageFeature>, Void>() {
			private static final long serialVersionUID = -5860317102985172799L;

			@Override
			public Void call(JavaPairRDD<ImageInfo, ImageFeature> v1)
					throws Exception {
				indexData(conn,  ind,  v1);
				return null;
			}
		});
	}

	@Override
	public JavaPairDStream<ImageFeature, ImageFeature> queryFeaturesStreaming(
			ReplicaConnection conn, IndexingParams params, JavaPairDStream<ImageInfo, ImageFeature> sketches) {
		final ReplicaConnection connF = conn;
		final IndexingParams paramsF = params;
		return sketches.transformToPair(new Function<JavaPairRDD<ImageInfo, ImageFeature>, JavaPairRDD<ImageFeature, ImageFeature>>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public JavaPairRDD<ImageFeature, ImageFeature> call(JavaPairRDD<ImageInfo, ImageFeature> v1) throws Exception {
				return queryFeatures(connF, paramsF, v1);
			}
		});
	}

	@Override
	public ImageInfo getImage(ReplicaConnection conn, long id) throws DataException {
		return this.images.lookup(id).get(0);
	}

}
