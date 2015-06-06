package org.twitterReplica.core;

import org.twitterReplica.model.SketchFunction;

import scala.Serializable;

public class IndexingParams implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4631497149930071141L;
	
	private SketchFunction hashFunc;
	private int numTables;
	private int hammingDistance;
	private boolean dataEncodingEmabled;
	private boolean compressionEnabled;
	private int ttl;
	
	public IndexingParams(SketchFunction hashFunc, int numTables,
			int hammingDistance, boolean dataEncodingEmabled,
			boolean compressionEnabled, int ttl) {
		super();
		this.hashFunc = hashFunc;
		this.numTables = numTables;
		this.hammingDistance = hammingDistance;
		this.dataEncodingEmabled = dataEncodingEmabled;
		this.compressionEnabled = compressionEnabled;
		this.ttl = ttl;
	}

	public int getNumTables() {
		return numTables;
	}

	public SketchFunction getSketchFunction() {
		return hashFunc;
	}

	public int getHammingDistance() {
		return hammingDistance;
	}

	public boolean isDataEncodingEnabled() {
		return dataEncodingEmabled;
	}

	public void setDataEncodingEmabled(boolean dataEncodingEmabled) {
		this.dataEncodingEmabled = dataEncodingEmabled;
	}

	public boolean isCompressionEnabled() {
		return compressionEnabled;
	}

	public void setCompressionEnabled(boolean compressionEnabled) {
		this.compressionEnabled = compressionEnabled;
	}

	public int getTTL() {
		return ttl;
	}

}
