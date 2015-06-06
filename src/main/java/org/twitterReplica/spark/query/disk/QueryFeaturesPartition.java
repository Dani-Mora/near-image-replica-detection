package org.twitterReplica.spark.query.disk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.twitterReplica.core.IndexingParams;
import org.twitterReplica.core.ReplicaConnection;
import org.twitterReplica.data.hbase.HBaseClient;
import org.twitterReplica.exceptions.DataException;
import org.twitterReplica.model.ImageFeature;
import org.twitterReplica.model.ImageInfo;

import scala.Tuple2;

public class QueryFeaturesPartition implements PairFlatMapFunction<Iterator<Tuple2<ImageInfo, ImageFeature>>, ImageFeature, ImageFeature> {

	
	/**
	 * 
	 */
	private static final long serialVersionUID = -3160942127594877083L;
	
	private IndexingParams params;
	private ReplicaConnection conn;
	
	public QueryFeaturesPartition(IndexingParams params, ReplicaConnection cParams) {
		this.params = params;
		this.conn = cParams;
	}

	public Iterable<Tuple2<ImageFeature, ImageFeature>> call(Iterator<Tuple2<ImageInfo,ImageFeature>> t) throws Exception {

		Configuration conf = HBaseClient.getConfiguration(this.conn);
		HConnection connection = null;
		HTableInterface hashTables[] = null;
		int numTables = params.getNumTables();
		int hammingDist = params.getHammingDistance();
		int minimumTables = numTables - hammingDist;
		List<Tuple2<ImageFeature, ImageFeature>> result = new ArrayList<Tuple2<ImageFeature, ImageFeature>>();
		
		try {
			// Start connection
			connection = HConnectionManager.createConnection(conf);
			// Connect to hash tables
			hashTables = HBaseClient.getSketchTableConnections(connection, params.getNumTables());
			
			// Go through all features in the partition
			while(t.hasNext()) {
				
				ImageFeature currentFeature = t.next()._2;
				
				// Map with table hits per feature
				Map<String, Integer> featMatches = new HashMap<String, Integer>();
					
				// Get matched feature for the input feature and the number of tables matched
				for (int i = 0; i < params.getNumTables(); ++i) {
					// Update matches so far
					HBaseClient.queryTable(currentFeature, currentFeature.getSketch().toInt()[i], hashTables[i], featMatches, minimumTables, result);
				}
			}
				
			return result;

		} catch (IOException e) {
			throw new DataException("Initialization error: " + e.getMessage());
		} finally {
			HBaseClient.closeHashTables(hashTables);
			HBaseClient.close(connection);
		 }
	}

}
