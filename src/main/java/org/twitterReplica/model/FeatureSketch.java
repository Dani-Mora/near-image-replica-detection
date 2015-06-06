package org.twitterReplica.model;

import org.apache.spark.util.collection.BitSet;
import org.twitterReplica.exceptions.InvalidArgumentException;
import org.twitterReplica.utils.ReplicaUtils;

import scala.Serializable;


public class FeatureSketch implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8350112738189071275L;
	
	private BitSet[] blocks; // Computed blocks
	
	public FeatureSketch(BitSet[] hashes) {
		super();
		this.blocks = hashes;
	}
	
	public BitSet[] getHashes() {
		return blocks;
	}
	
	public static FeatureSketch fromInt(int[] hashes) {
		BitSet[] blocks = new BitSet[hashes.length];
		for (int i = 0; i < hashes.length; ++i) {
			blocks[i] = ReplicaUtils.fromInt(hashes[i]);
		}
		return new FeatureSketch(blocks);
	}
	
	public int[] toInt() {
		int[] blocksInt = new int[this.blocks.length];
		for (int i = 0; i < this.blocks.length; ++i) {
			blocksInt[i] = ReplicaUtils.toInt(this.blocks[i]);
		}
		return blocksInt;
	}
	
	/*	
	 * 	Returns the string representation of the whole sketch
	 */
	public String getStringFromBlocks() throws InvalidArgumentException {
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < this.blocks.length; ++i) {
			String blockStr = String.valueOf(ReplicaUtils.toInt(blocks[i]));
			builder.append(blockStr);
		}
		return builder.toString();
	}
	
	/*	
	 * 	Returns the string representation of the whole sketch, where blocks are separated by the given
	 * 	character
	 * 	@param separator Separator character between blocks
	 */
	public String getStringFromBlocksSeparated(char separator) throws InvalidArgumentException {
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < this.blocks.length; ++i) {
			String blockStr = String.valueOf(ReplicaUtils.toInt(blocks[i]));
			if (i == 0) {
				builder.append(blockStr);
			}
			else {
				builder.append(separator + blockStr);
			}
		}
		return builder.toString();
	}
	
	/*	
	 * 	Returns the string representation of the i-th block of the sketch
	 */
	public String getStringFromBlock(int i) throws InvalidArgumentException {
		if (i >= this.blocks.length) {
			throw new InvalidArgumentException("Requested block " + i + " but max index is " + this.blocks.length);
		}
		return String.valueOf(ReplicaUtils.toInt(blocks[i]));
	}
	
	/*	
	 * 	Returns the integer representation of the i-th block of the sketch
	 */
	public int getKey(int i) {
		return ReplicaUtils.toInt(this.blocks[i]);
	}
	
}
