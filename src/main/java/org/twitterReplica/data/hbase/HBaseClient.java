package org.twitterReplica.data.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.util.collection.BitSet;
import org.twitterReplica.core.DescriptorParams;
import org.twitterReplica.core.FilteringParams;
import org.twitterReplica.core.IndexingParams;
import org.twitterReplica.core.ReplicaConnection;
import org.twitterReplica.exceptions.DataException;
import org.twitterReplica.exceptions.InvalidArgumentException;
import org.twitterReplica.exceptions.NotFoundException;
import org.twitterReplica.model.DescriptorType;
import org.twitterReplica.model.FeatureSketch;
import org.twitterReplica.model.FilteringType;
import org.twitterReplica.model.ImageFeature;
import org.twitterReplica.model.ImageInfo;
import org.twitterReplica.model.SketchFunction;
import org.twitterReplica.model.keypoints.KeypDetectors;
import org.twitterReplica.model.providers.ProviderType;
import org.twitterReplica.utils.ReplicaUtils;

import scala.Serializable;
import scala.Tuple2;

public class HBaseClient implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	// Tables
	private static final String HASH_TABLE_PREFIX = "ht";
	private static final String PARAMS_TABLE = "pt";
	private static final String IMG_TABLE = "it";
	
	// Image table
	private static final String IMG_FAMILY = "imf";
	private static final String PATH_Q = "pq";
	private static final String PROVIDER_Q = "prq";
	private static final String RESOURCE_Q = "rq";
	
	// Hash table
	private static final String HASH_FAMILY = "hf";
	private static final String FEATURE_ID_Q = "fq";
	private static final char SEPARATOR = '_';
	private static final char SEPARATOR_QUERY = '.';
	
	// Params table
	private static final String DESCRIPTOR_FAMILY = "df";
	private static final String DESC_TYPE_Q = "dtq";
	private static final String KEYP_TYPE_Q = "kyq";
	private static final String MAX_SIDE_Q = "msq";
	private static final String FILTERING_FAMILY = "ff";
	private static final String FILT_TYPE_Q = "ftq";
	private static final String THRESHOLD_Q = "tq";
	private static final String LOG_SCALE_Q = "lsq";
	private static final String INDEXING_FAMILY = "if";
	private static final String NUM_TABLES_Q = "ntq";
	private static final String HAMMING_DIST_Q = "hdq";
	private static final String W_Q = "wq";
	private static final String A_Q = "aq";
	private static final String B_Q = "bq";
	private static final short PARAM_ROW = 1;
	private static final String DATA_BLOCK_ENCODING_Q = "dbeq";
	private static final String COMPRESSION_Q = "cq";
	private static final String TTL_Q = "ttlq";
	
	static Logger logger = Logger.getLogger(HBaseClient.class);
	
	public static HConnection getConnection(Configuration conf) throws IOException {
		return HConnectionManager.createConnection(conf);
	}
	
	public static Configuration getConfiguration(ReplicaConnection conn) {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", conn.getThirdParam());
		conf.set("hbase.zookeeper.property.clientPort", conn.getSecondParam());
		conf.set("hbase.master", conn.getFirstParam());
		return conf;
	}
	
	/*
	 * 	Creates the meta data tables and the indexing structures (empty)
	 * 	@param conf HBase configuration
	 * 	@param params Indexing parameters
	 */
	public static void reset(Configuration conf, IndexingParams params) throws DataException {
		
		HConnection connection = null;
		HBaseAdmin admin = null;
		
		try {
			connection = getConnection(conf);
			admin = new HBaseAdmin(conf);
			
			// Resets parameters table
			HBaseUtils.deleteTable(admin, PARAMS_TABLE);
			HBaseUtils.createTable(admin, PARAMS_TABLE,
					new String[] { DESCRIPTOR_FAMILY, FILTERING_FAMILY, INDEXING_FAMILY},
					false, false, -1);
			
			// Reset rest of tables
			resetData(conf, params);

		} catch (IOException e) {
			throw new DataException("Initialization error: " + e.getMessage());
		} finally {
			close(connection, admin);
		 }
	}
	
	/*
	 * 	Flushes the indexing structures (empty)
	 * 	@param conf HBase configuration
	 * 	@param params Indexing parameters
	 */
	public static void resetData(Configuration conf, IndexingParams params) throws DataException {
		
		HConnection connection = null;
		HBaseAdmin admin = null;
		
		try {
			connection = getConnection(conf);
			admin = new HBaseAdmin(conf);
			
			// Delete tables
			HBaseUtils.deleteHashTables(admin, params.getNumTables(), HASH_TABLE_PREFIX);
			HBaseUtils.deleteTable(admin, IMG_TABLE);
			
			// Create 'image' table
			HBaseUtils.createTable(admin, IMG_TABLE, new String[] { IMG_FAMILY }, 
					 params.isDataEncodingEnabled(), params.isCompressionEnabled(), params.getTTL());
			
			// Create hash tables
			HBaseUtils.createHashTables(admin, HASH_TABLE_PREFIX, params.getNumTables(), 
					new String[] { HASH_FAMILY },  params.isDataEncodingEnabled(), params.isCompressionEnabled()
					,params.getTTL());

		} catch (IOException e) {
			throw new DataException("Initialization error: " + e.getMessage());
		} finally {
			close(connection, admin);
		 }
	}
	
	/*
	 * 	Saves image metadata information
	 * 	@param conf HBase configuration
	 * 	@param info Image metadata
	 */
	protected static void saveImageMetadata(HTableInterface table, ImageInfo img) throws IOException {
		Put p = new Put(Bytes.toBytes(img.getId()));
		p.add(Bytes.toBytes(IMG_FAMILY), Bytes.toBytes(PATH_Q), Bytes.toBytes(img.getPath()));
		p.add(Bytes.toBytes(IMG_FAMILY), Bytes.toBytes(PROVIDER_Q), Bytes.toBytes(img.getProvider().toString()));
		p.add(Bytes.toBytes(IMG_FAMILY), Bytes.toBytes(RESOURCE_Q), Bytes.toBytes(img.getResourceId()));
		table.put(p);
	}
	
	/*
	 * 	Indexes the given features belonging to the input image
	 * 	@param conf HBase configuration
	 * 	@param imgId Image the features belong to
	 * 	@param params Indexing parameters
	 * 	@param features Features to index
	 */
	public static void save(Configuration conf, ImageInfo img, IndexingParams params, Iterable<ImageFeature> features) throws DataException {
		
		HConnection conn = null;
		HTableInterface[] sketchTables = new HTable[params.getNumTables()];
		HTableInterface imgTable = null;
		try {
			// Create connection
			getConnection(conf);
			// Connect to tables
			sketchTables = getSketchTableConnections(conn, params.getNumTables());
			// Connect to image table
			imgTable = getImageTableConnection(conf);
			// Store data metadata
			save(sketchTables, imgTable, img, features, params);

		} catch (IOException e) {
			throw new DataException("Error indexing image: " + e.getMessage());
		} catch (InvalidArgumentException e) {
			throw new DataException("Error storing hashes: " + e.getMessage());
		} finally {
			for (int i = 0; i < params.getNumTables(); ++i) {
				close(sketchTables[i]);
			}
			close(imgTable);
		 }
	}
	
	/*
	 * 	Stores feature sketches and image metadata. Connections must be manually closed afterwards
	 * 	@param conf HBase configuration
	 * 	@param sketchTables Connection to sketches tables
	 * 	@param imgTable Connection to image table
	 * 	@param img Image metadata
	 * 	@param features Sketched features
	 * 	@param params Indexing parameters
	 */
	public static void save(HTableInterface[] skecthTables, HTableInterface imgTable, ImageInfo img, 
			Iterable<ImageFeature> features, IndexingParams params) 
					throws InvalidArgumentException, IOException {
		
		// Save image metadata
		saveImageMetadata(imgTable, img);
		
		// Save sketches
		long imgId = img.getId();
		// For each feature, store it
		for (ImageFeature feature : features) {

			// Build feature key
			String featureKey = feature.getSketch().getStringFromBlocksSeparated(SEPARATOR) 
					+ SEPARATOR + String.valueOf(imgId);
			
			// For each table, store the corresponding key
			for (int i = 0; i < params.getNumTables(); ++i) {
				// Build row with hash block as key
				String key_str = feature.getSketch().getStringFromBlock(i) 
						+ SEPARATOR + String.valueOf(imgId);
				Put p = new Put(Bytes.toBytes(key_str));
				
				// Save feature identifier
				p.add(Bytes.toBytes(HASH_FAMILY), Bytes.toBytes(FEATURE_ID_Q), 
						Bytes.toBytes(featureKey));
				
				// Store row
				skecthTables[i].put(p);
			}
		}
	}
	
	/*
	 * 	QUERY FUNCTIONS
	 */
	
	/*
	 * 	Queries the given image into the HBase storage and returns the matches features
	 * 	@param conf HBase configuration
	 * 	@param feature List of features to index
	 * 	@param params Indexing parameters
	 */
	public static List<Tuple2<ImageFeature, ImageFeature>>
		query(Configuration conf, Iterable<ImageFeature> features, IndexingParams params) 
				throws DataException {
		
		HConnection connection = null;
		HTableInterface hashTables[] = null;
		int numTables = params.getNumTables();
		int hammingDist = params.getHammingDistance();
		int minimumTables = numTables - hammingDist;
		List<Tuple2<ImageFeature, ImageFeature>> result = new ArrayList<Tuple2<ImageFeature, ImageFeature>>();
		
		try {
			connection = HConnectionManager.createConnection(conf);
			// Connect to hash tables
			hashTables = getSketchTableConnections(connection, params.getNumTables());

			// For each feature, query
			for (ImageFeature feature : features) {
				
				// Map with table hits per feature
				Map<String, Integer> featMatches = new HashMap<String, Integer>();
				
				// Get matched feature for the input feature and the number of tables matched
				for (int i = 0; i < params.getNumTables(); ++i) {
					// Update matches so far
					queryTable(feature, feature.getSketch().toInt()[i], hashTables[i], featMatches, minimumTables, result);
				}
			}
			
			return result;

		} catch (IOException e) {
			throw new DataException("Initialization error: " + e.getMessage());
		} finally {
			closeHashTables(hashTables);
			close(connection);
		 }
	}
	
	/*
	 * 	Returns the list of matches for the given key
	 * 	@param query Image feature queried
	 * 	@param key Key of the block to query
	 * 	@param hashTable Hash table
	 * 	@param matched Matches found for the given query in the previous tables
	 * 	@param min Minimum number of tables to match to consider candidat
	 * 	@param result Feature matches
	 */
	public static void queryTable(ImageFeature query, int key, HTableInterface hashTable, 
			Map<String, Integer> matched, int min, List<Tuple2<ImageFeature, ImageFeature>> result) throws IOException {
		
		// Define start and end row
		Scan scanOp = buildScanner(key);
		ResultScanner scanner = hashTable.getScanner(scanOp);
		
		// Get results
		for (Result scanResult = scanner.next(); scanResult != null; scanResult = scanner.next()) {
			
			// Get feature id
			String id = getImgIdFromResult(scanResult);
			
			// Set to 0 if key not found
			if (!matched.containsKey(id)) {
				// Initialize with 1
				matched.put(id, 0);
			}

			// Increment if already not a candidate
			int currentMatches = matched.get(id);
			if (currentMatches < min) {
				
				// Update match
				int updated = currentMatches + 1;
				matched.put(id, updated);
				
				// Add to candidate if needed
				if (updated == min) {
					ImageFeature candidate = getFeatureFromId(id);
					result.add(new Tuple2<ImageFeature, ImageFeature> (query, candidate));
				}
			}
		}
	}
	
	/*
	 * 	PARAMETERS FUNCTIONS
	 */
	
	/*
	 * 	Reads the descriptor parameters from the data base
	 * 	@return Descriptor parameters read from the data base
	 */
	public static DescriptorParams readDescriptorParams(Configuration conf) throws NotFoundException, DataException {
		
		HConnection connection = null;
		HTable table = null;
		
		try {
			connection = HConnectionManager.createConnection(conf);
			table = new HTable(conf, PARAMS_TABLE);
			
			// Read first and unique row
			Scan scanner = new Scan();
			ResultScanner result = table.getScanner(scanner);
			Result res = result.next();
			
			if (res == null) {
				throw new NotFoundException("Descriptor parameters not found");
			}
			
			// Read descriptor params
			DescriptorType descT = DescriptorType.valueOf(Bytes.toString(
						res.getValue(Bytes.toBytes(DESCRIPTOR_FAMILY), Bytes.toBytes(DESC_TYPE_Q))));
			KeypDetectors keyp = KeypDetectors.valueOf(Bytes.toString(
					res.getValue(Bytes.toBytes(DESCRIPTOR_FAMILY), Bytes.toBytes(KEYP_TYPE_Q))));
			int maxSide = Bytes.toInt(
					res.getValue(Bytes.toBytes(DESCRIPTOR_FAMILY), Bytes.toBytes(MAX_SIDE_Q)));
			return new DescriptorParams(descT, keyp, maxSide);
			
		} catch (IOException e) {
			throw new DataException("Error reading parameters: " + e.getMessage());
		} finally {
			close(connection, table);
		 }
	}
	
	/*
	 * 	Reads the filtering parameters from the data base
	 * 	@return Filtering parameters stored in the data base
	 */
	public static FilteringParams readFilteringParams(Configuration conf) throws NotFoundException, DataException {
		
		HConnection connection = null;
		HTable table = null;
		
		try {
			connection = HConnectionManager.createConnection(conf);
			table = new HTable(conf, PARAMS_TABLE);
			
			// Read first and unique row
			Scan scanner = new Scan();
			ResultScanner result = table.getScanner(scanner);
			Result res = result.next();
			
			if (res == null) {
				throw new NotFoundException("Filtering parameters not found");
			}
			
			// Read filtering params
			FilteringType fType = FilteringType.valueOf(Bytes.toString(
					res.getValue(Bytes.toBytes(FILTERING_FAMILY), Bytes.toBytes(FILT_TYPE_Q))));
			double thresh = Bytes.toDouble(
				res.getValue(Bytes.toBytes(FILTERING_FAMILY), Bytes.toBytes(THRESHOLD_Q)));
			boolean logScale = Bytes.toBoolean(
					res.getValue(Bytes.toBytes(FILTERING_FAMILY), Bytes.toBytes(LOG_SCALE_Q)));
			return new FilteringParams(fType, thresh, logScale);
			
		} catch (IOException e) {
			throw new DataException("Error reading parameters: " + e.getMessage());
		} finally {
			close(connection, table);
		 }
	}
	
	/*
	 * 	Reads the indexing parameters from the data base
	 * 	@return Indexing parameters stored in the data base
	 */
	public static IndexingParams readIndexingParams(Configuration conf) throws NotFoundException, DataException {
		
		HConnection connection = null;
		HTable table = null;
		
		try {
			connection = HConnectionManager.createConnection(conf);
			table = new HTable(conf, PARAMS_TABLE);
			
			// Read first and unique row
			Scan scanner = new Scan();
			ResultScanner result = table.getScanner(scanner);
			Result res = result.next();
			
			if (res == null) {
				throw new NotFoundException("Indexing parameters not found");
			}
			
			// Read number of tables
			int tables = Bytes.toInt(res.getValue(Bytes.toBytes(INDEXING_FAMILY), Bytes.toBytes(NUM_TABLES_Q)));
			
			// Read sketch function
			double[] a = ReplicaUtils.getArrayFromString(	Bytes.toString(res.getValue(Bytes.toBytes(INDEXING_FAMILY), Bytes.toBytes(A_Q))));
			double b = Bytes.toDouble(res.getValue(Bytes.toBytes(INDEXING_FAMILY), Bytes.toBytes(B_Q)));
			int W = Bytes.toInt(res.getValue(Bytes.toBytes(INDEXING_FAMILY), Bytes.toBytes(W_Q)));
			
			// Read hamming threshold
			int hammingDist = Bytes.toInt(res.getValue(Bytes.toBytes(INDEXING_FAMILY), Bytes.toBytes(HAMMING_DIST_Q)));
			
			// Read data compression params
			boolean blockEncoding = Bytes.toBoolean(res.getValue(Bytes.toBytes(INDEXING_FAMILY), Bytes.toBytes(DATA_BLOCK_ENCODING_Q)));
			boolean compression = Bytes.toBoolean(res.getValue(Bytes.toBytes(INDEXING_FAMILY), Bytes.toBytes(COMPRESSION_Q)));
			int ttl = Bytes.toInt(res.getValue(Bytes.toBytes(INDEXING_FAMILY), Bytes.toBytes(TTL_Q)));
			
			return new IndexingParams(new SketchFunction(a, b, W), tables, hammingDist, 
					blockEncoding, compression, ttl);
			
		} catch (IOException e) {
			throw new DataException("Error reading parameters: " + e.getMessage());
		} finally {
			close(connection, table);
		 }
	}
	
	/*
	 * 	Stores the descriptor, filtering and indexing parameters into the data base
	 * 	@param descP Destination parameters where to store  the descriptor parameters
	 * 	@param filtP Destination parameters where to store  the filtering parameters
	 * 	@param indexingP Destination parameters where to store  the indexing parameters
	 */
	public static void storeParameters(Configuration conf, DescriptorParams descP,
			FilteringParams filtP, IndexingParams indexingP) 
			throws DataException {
		
		HConnection connection = null;
		HTable table = null;
		
		try {
			connection = HConnectionManager.createConnection(conf);
			table = new HTable(conf, PARAMS_TABLE);
			
			Put p = new Put(Bytes.toBytes(PARAM_ROW));
			
			// Add descriptor values
			p.add(Bytes.toBytes(DESCRIPTOR_FAMILY), Bytes.toBytes(DESC_TYPE_Q), 
					Bytes.toBytes(descP.getDescriptorType().toString()));
			p.add(Bytes.toBytes(DESCRIPTOR_FAMILY), Bytes.toBytes(KEYP_TYPE_Q), 
					Bytes.toBytes(descP.getKeypointType().toString()));
			p.add(Bytes.toBytes(DESCRIPTOR_FAMILY), Bytes.toBytes(MAX_SIDE_Q), 
					Bytes.toBytes(descP.getMaximumLargestSide()));

			// Add filtering values
			p.add(Bytes.toBytes(FILTERING_FAMILY), Bytes.toBytes(FILT_TYPE_Q), 
					Bytes.toBytes(filtP.getFilteringType().toString()));
			p.add(Bytes.toBytes(FILTERING_FAMILY), Bytes.toBytes(THRESHOLD_Q), 
					Bytes.toBytes(filtP.getThresh()));
			p.add(Bytes.toBytes(FILTERING_FAMILY), Bytes.toBytes(LOG_SCALE_Q), 
					Bytes.toBytes(filtP.isLogScaleEnabled()));
			
			// Add indexing values
			p.add(Bytes.toBytes(INDEXING_FAMILY), Bytes.toBytes(NUM_TABLES_Q), 
					Bytes.toBytes(indexingP.getNumTables()));
			p.add(Bytes.toBytes(INDEXING_FAMILY), Bytes.toBytes(A_Q), 
					Bytes.toBytes(ReplicaUtils.listToString(indexingP.getSketchFunction().getA())));
			p.add(Bytes.toBytes(INDEXING_FAMILY), Bytes.toBytes(B_Q), 
					Bytes.toBytes(indexingP.getSketchFunction().getB()));
			p.add(Bytes.toBytes(INDEXING_FAMILY), Bytes.toBytes(W_Q), 
					Bytes.toBytes(indexingP.getSketchFunction().getW()));
			p.add(Bytes.toBytes(INDEXING_FAMILY), Bytes.toBytes(HAMMING_DIST_Q), 
					Bytes.toBytes(indexingP.getHammingDistance()));
			p.add(Bytes.toBytes(INDEXING_FAMILY), Bytes.toBytes(DATA_BLOCK_ENCODING_Q), 
					Bytes.toBytes(indexingP.isDataEncodingEnabled()));
			p.add(Bytes.toBytes(INDEXING_FAMILY), Bytes.toBytes(COMPRESSION_Q), 
					Bytes.toBytes(indexingP.isCompressionEnabled()));
			p.add(Bytes.toBytes(INDEXING_FAMILY), Bytes.toBytes(TTL_Q), 
					Bytes.toBytes(indexingP.getTTL()));
					
			// Store row
			table.put(p);
			
		} catch (IOException e) {
			throw new DataException("Error storing parameters: " + e.getMessage());
		} finally {
			close(connection, table);
		 }
	}
	
	/*
	 * 	Gets the information for an image with the given identifier
	 * 	@param conf HBase configuration
	 * 	@param id Image identifier
	 */
	public static ImageInfo getImage(Configuration conf, Long id) throws DataException {
		
		HConnection connection = null;
		HTable table = null;
		
		try {
			connection = HConnectionManager.createConnection(conf);
			table = new HTable(conf, IMG_TABLE);
			
			Get get = new Get(Bytes.toBytes(id));
			Result res = table.get(get);
			return getImageFromResult(res);
			
		} catch (IOException e) {
			throw new DataException("Error reading image: " + e.getMessage());
		} finally {
			close(connection, table);
		 }
	}
	
	/*
	 * 	Auxiliar functions
	 */
	
	public static void close(HConnection conn, HTableInterface table) {
		close(table);
		close(conn);
	}
	
	public static void close(HTableInterface table) {
		if (table != null) {
			try {
				table.close();
			} catch (IOException e) {
				logger.warn("Error closing HTableInterface: " + e.getMessage());
			}
		}
	}
	
	public static void close(HConnection conn) {
		if (conn != null) {
			try {
				conn.close();
			} catch (IOException e) {
				logger.warn("Error closing HBase connection: " + e.getMessage());
			}
		}
	}
	
	protected static void close(HConnection conn, HBaseAdmin admin) {
		if (conn != null) {
			try {
				conn.close();
			} catch (IOException e) {
				logger.warn("Error closing HBase connection: " + e.getMessage());
			}
		}
		if (admin != null) {
			try {
				admin.close();
			} catch (IOException e) {
				logger.warn("Error closing HBase connection: " + e.getMessage());
			}
		}
	}
	
	/*	
	 *  Closes the connections to hash tables
	 *  @param tables Array of hash table connections
	 */
	public static void closeHashTables(HTableInterface[] tables) {
		for (int i = 0; i < tables.length; ++i) {
			try {
				if (tables[i] != null) {
					tables[i].close();
				}
			} catch (IOException e) {
				logger.warn("Error closing HBase connection: " + e.getMessage());
			}
		}
	}
	
	/*
	 * 	Connection functions
	 */
	
	public static HTableInterface[] getSketchTableConnections(HConnection conn, int numTables) throws IOException {
		return HBaseUtils.connectToHashTables(conn, numTables, HASH_TABLE_PREFIX);
	}
	
	public static HTable getImageTableConnection(Configuration conf) throws IOException {
		return  new HTable(conf, IMG_TABLE);
	}
	
	/*
	 * 	Scan functions
	 */
	
	protected static String getStartRow(int key) {
		return String.valueOf(key) + SEPARATOR;
	}
	
	protected static String getEndRow(int key) {
		// Define start and end row
		int end = key + 1;
		if (end == Integer.MAX_VALUE) {
			return null;
		}
		else {
			return String.valueOf(end) + SEPARATOR_QUERY;
		}
	}
	
	/*
	 * 	Builds the proper HBase object for scanning input key in a hash table
	 * 	@paran key Key to seek
	 */
	private static Scan buildScanner(int key) {
		// Define start and end row
		String start_str = getStartRow(key);
		String end_str = getEndRow(key);

		Scan scanOp = null;
		if (end_str == null) {
			scanOp = new Scan(Bytes.toBytes(start_str));
		}
		else {
			scanOp = new Scan(Bytes.toBytes(start_str), Bytes.toBytes(end_str));
		}
		
		// Get & add results
		scanOp.addColumn(Bytes.toBytes(HASH_FAMILY), Bytes.toBytes(FEATURE_ID_Q));
		
		return scanOp;
	}
	
	/*
	 * 	Result parsing functions
	 */
	
	protected static ImageInfo getImageFromResult(Result res) {
		String path = Bytes.toString(res.getValue(Bytes.toBytes(IMG_FAMILY), Bytes.toBytes(PATH_Q)));
		ProviderType provider = ProviderType.fromString(Bytes.toString(res.getValue(Bytes.toBytes(IMG_FAMILY), Bytes.toBytes(PROVIDER_Q))));
		String resId = Bytes.toString(res.getValue(Bytes.toBytes(IMG_FAMILY), Bytes.toBytes(RESOURCE_Q)));
		long id = Bytes.toLong(res.getRow());
		return new ImageInfo(path, id, resId, provider);
	}
	
	protected static String getImgIdFromResult(Result res) {
		byte[] featureIdBytes = res.getValue(Bytes.toBytes(HASH_FAMILY), Bytes.toBytes(FEATURE_ID_Q));
		return Bytes.toString(featureIdBytes);
	}
	
	protected static ImageFeature getFeatureFromId(String id) {
		String[] splits = id.split("" + SEPARATOR);
	
		// Get sketch
		int blocks = splits.length - 1;
		BitSet[] hashes = new BitSet[blocks];
		for (int i = 0; i < blocks; ++i) {
			hashes[i] = ReplicaUtils.fromInt(Integer.valueOf(splits[i]));
		}
		
		// Image id
		long imgId = Long.valueOf(splits[splits.length -1]);
		
		return new ImageFeature(imgId, new FeatureSketch(hashes));
	}
}
