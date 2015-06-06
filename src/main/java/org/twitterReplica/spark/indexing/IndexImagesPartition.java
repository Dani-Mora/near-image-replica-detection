package org.twitterReplica.spark.indexing;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.spark.api.java.function.VoidFunction;
import org.twitterReplica.core.IndexingParams;
import org.twitterReplica.core.ReplicaConnection;
import org.twitterReplica.data.hbase.HBaseClient;
import org.twitterReplica.exceptions.DataException;
import org.twitterReplica.exceptions.InvalidArgumentException;
import org.twitterReplica.model.ImageFeature;
import org.twitterReplica.model.ImageInfo;

import scala.Tuple2;

/*
 * 	Void fucntion that indexes information into HBase. It opens one connection to the database for each
 * 	partition in the system to avoid overhead for high number of open connections.
 */
public class IndexImagesPartition implements VoidFunction<Iterator<Tuple2<ImageInfo, Iterable<ImageFeature>>>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5723151836130679315L;

	private IndexingParams indParams;
	private ReplicaConnection conn;
	
	public IndexImagesPartition(IndexingParams indParams, ReplicaConnection cParams) {
		this.indParams = indParams;
		this.conn = cParams;
	}
	
	public void call(Iterator<Tuple2<ImageInfo, Iterable<ImageFeature>>> t) throws Exception {
		Configuration conf = HBaseClient.getConfiguration(this.conn);
		HConnection conn = null;
		int numTables = this.indParams.getNumTables();
		HTableInterface[] sketchTables = new HTableInterface[numTables];
		HTableInterface imgTable = null;
		
		try {
			
			conn = HBaseClient.getConnection(conf);
			
			// Connect to tables
			sketchTables = HBaseClient.getSketchTableConnections(conn, numTables);
			imgTable = HBaseClient.getImageTableConnection(conf);
			
			while(t.hasNext()) {
				Tuple2<ImageInfo, Iterable<ImageFeature>> currentImage = t.next();
				HBaseClient.save(sketchTables, imgTable, currentImage._1, currentImage._2, this.indParams);
			}

		} catch (IOException e) {
			throw new DataException("Error indexing image: " + e.getMessage());
		} catch (InvalidArgumentException e) {
			throw new DataException("Error storing hashes: " + e.getMessage());
		} finally {
			HBaseClient.closeHashTables(sketchTables);
			HBaseClient.close(conn, imgTable);
		 }
	}
		
}
