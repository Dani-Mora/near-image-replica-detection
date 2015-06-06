package org.twitterReplica.data.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;

public class HBaseUtils {

	/*
	 * 	Deletes HBase table if it already exists
	 * 	@param admin HBase admin
	 * 	@param name Name of the table
	 */
	public static void deleteTable(HBaseAdmin admin, String name) throws IOException {
		if (admin.tableExists(TableName.valueOf(name))) {
			admin.disableTable(TableName.valueOf(name));
			admin.deleteTable(TableName.valueOf(name));
		}
	}
	
	/*
	 * 	Deletes key tables if they already exist
	 * 	@param admin HBase admin
	 * 	@param name Name 
	 */
	public static void deleteHashTables(HBaseAdmin admin, int numTables, String prefix) 
			throws IOException {
		for (int i = 0; i < numTables; ++i) {
			String tableName = prefix + String.valueOf(i);
			deleteTable(admin, tableName);
		}
	}
	
	/*
	 * 	Create table given the family descriptors
	 * 	@param admin HBase admin
	 * 	@param param tableName Name of the table
	 * 	@param families Array of family names
	 * 	@param dataBlockEncoding Whether to encode the column families
	 * 	@param compression Whether to enable compression on columns
	 * 	@param remaining Seconds the rows are alive. -1 to never delete them
	 */
	public static void createTable(HBaseAdmin admin, String tableName, String[] families,
			boolean dataBlockEncoding, boolean compression, int remaining) throws IOException {
		HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
		for (String f : families) {
			HColumnDescriptor colDesc = new HColumnDescriptor(f);
			// Compression
			if (compression) {
				colDesc.setCompressionType(Algorithm.SNAPPY);
			}
			
			// Alive time
			if (remaining > 0) {
				colDesc.setTimeToLive(remaining);
			}
			
			// Block encoding
			if (dataBlockEncoding) {
				colDesc.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
			}
			
			table.addFamily(colDesc);
		}
		admin.createTable(table);
	}
	
	/*
	 * 	Creates the hash tables
	 * 	@param admin HBase admin
	 * 	@param param prefix Hash table prefix name
	 * 	@param num Number of hash tables to create
	 * 	@param families Array of family names
	 * 	@param blockEnc Whether block encoding must be enabled
	 * 	@param compr Whether compression must be enabled
	 * 	@param ttl Time to live parameter
	 * 	@param inMem Persistence mode of the system
	 */
	public static void createHashTables(HBaseAdmin admin, String prefix, int num, String[] families, 
			boolean blockEnc, boolean compr, int ttl) 
			throws IOException {
		for (int i = 0 ; i < num; ++i) {
			String tableName = prefix + String.valueOf(i);
			createTable(admin, tableName, families, blockEnc, compr, ttl);
		}
	}
	
	/*	
	 *  Connects to hash tables
	 *  @param conf HBase connection
	 *  @param num Number of hash tables
	 *  @param prefix Hash table prefix
	 *  @return connection to all hash tables
	 */
	public static HTableInterface[] connectToHashTables(HConnection conn, int num, String prefix) 
			throws IOException {
		HTableInterface[] hashTables = new HTable[num];
		for (int i = 0; i < num; ++i) {
			hashTables[i] = conn.getTable(prefix + String.valueOf(i));
		}
		return hashTables;
	}
	
	
}
