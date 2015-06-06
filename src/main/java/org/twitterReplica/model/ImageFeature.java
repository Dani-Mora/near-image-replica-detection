package org.twitterReplica.model;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.util.collection.BitSet;
import org.twitterReplica.exceptions.InvalidArgumentException;

import scala.Serializable;

public class ImageFeature implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5201927053740639232L;
	static Logger logger = Logger.getLogger(ImageFeature.class);
	
	private static final int BLOCK_SIZE = 32;
	private static final double log10_1 = Math.log10(1);
	private static final double log10_2 = Math.log10(2);
	private static final double log2 = Math.log(2);
	
	private long imgId;
	private double[] feature;

	// Hashing info
	private FeatureSketch sketch = null;
	private DescriptorType featureType;
	
	public ImageFeature(long imgId, FeatureSketch sketch) {
		super();
		this.imgId = imgId;
		this.sketch = sketch;
	}
	
	public ImageFeature(long imgId, double[] feature, DescriptorType featureType) {
		super();
		this.imgId = imgId;
		this.feature = feature;
		this.featureType = featureType;
	}
	
	public ImageFeature(long imgId, double[] feature, FeatureSketch sketch, DescriptorType featureType) {
		this(imgId, feature, featureType);
		this.sketch = sketch;
	}
	
	public double[] getFeature() {
		return feature;
	}
	
	public DescriptorType getFeatureType() {
		return featureType;
	}

	public void setFeatureType(DescriptorType featureType) {
		this.featureType = featureType;
	}
	
	public FeatureSketch getSketch() {
		return sketch;
	}

	public void setSketch(FeatureSketch sketch) {
		this.sketch = sketch;
	}
	
	public long getImgId() {
		return imgId;
	}

	public void setImgId(long imgId) {
		this.imgId = imgId;
	}
	
	public void clearVector() {
		for (int i = 0; i < this.feature.length; ++i) {
			feature[i] = 0;
		}
	}

	/*
	 * 	Computes sketch mapping each feature value into one bit
	 */
	public void computeSketch(SketchFunction func, int blocks)	throws InvalidArgumentException {
			
		int featureSize = DescriptorType.getSize(this.featureType);
		double[] feat = this.feature;
		
		// For sketching SURF features, must be converted into [0, 255]
		if (this.featureType.equals(DescriptorType.SURF)) {
			// SURF features are discretized and converted into new range
			double oldMin = DescriptorType.getMinimumValue(this.featureType);
			double oldMax = DescriptorType.getMaximumValue(this.featureType);
			feat = modifyRange(this.feature, oldMin, oldMax, 0, 255);
		}
		
		// Get rounded size so we make sure it always fits
		int bitsPerBlock = (int) Math.round((double)featureSize/(double)blocks);
		
		if (bitsPerBlock > BLOCK_SIZE) {
			throw new InvalidArgumentException("Block too big: " + bitsPerBlock 
					+ ". Maximum size is :" + BLOCK_SIZE);
		}
		
		if (featureSize % bitsPerBlock != 0) {
			throw new InvalidArgumentException("Feature size must be a multiple of "
					+ " the block size");
		}
			
		BitSet[] sketches = new BitSet[blocks];
		// For each of the k bits selected
		for (int i = 0; i < featureSize; ++i) {
			
			// Compute hash value
			int block = (int) i / bitsPerBlock;
			if (i % bitsPerBlock == 0) {
				sketches[block] = new BitSet(bitsPerBlock);
			}
			double p = feat[i];
			double val =  Math.floor((func.getA()[i] * p + func.getB()) / func.getW());
			
			// Store
			if (val % 2 == 1) {
				// Build block
				sketches[block].set(i - block * bitsPerBlock);
			}
	
		}
		
		this.sketch = new FeatureSketch(sketches);
	}
	
	/*
	 * 	Log scales the value of the features
	 */
	public void logScale() {
		double oldMin = DescriptorType.getMinimumValue(this.featureType);
		double oldMax = DescriptorType.getMaximumValue(this.featureType);
		for (int i = 0; i < this.feature.length; ++i) {
				double currentValue = this.feature[i];
				double norm = modifyRange(currentValue, oldMin, oldMax, 0.0, 1.0);
				double log = Math.log10(norm + 1);
				double logScaled = modifyRange(log, log10_1, log10_2, oldMin, oldMax);
				this.feature[i] = logScaled;
		}
	}
	
	/*
	 * 	Modifies the range of a value given the old and new minimum and maximum values
	 * 	@param mat
	 * 	@param oldMin Minimum current value
	 * 	@param oldMax Maximum current value
	 * 	@param newMin New minimum value
	 * 	@param newMax New maximum value
	 */
	protected static double modifyRange(double value, double oldMin, double oldMax, double newMin, double newMax) {
		double oldRange = oldMax - oldMin;
		double newRange = newMax - newMin;
		double newValue = (((value - oldMin) * newRange) / oldRange) + newMin;
		return newValue;
	}
	
	/*
	 * 	Converts the range of a vector into a new range of values
	 * 	@param src Original vector
	 *  @param oldMin Minimum current value
	 * 	@param oldMax Maximum current value
	 * 	@param newMin New minimum value
	 * 	@param newMax New maximum value
	 * 	
	 */
	protected static double[] modifyRange(double[] src, double oldMin, double oldMax, double newMin, double newMax) {
		double[] result = new double[src.length];
		for (int i = 0; i < src.length; ++i) {
			result[i] = modifyRange(src[i], oldMin, oldMax, newMin, newMax);
		}
		return result;
	}
	
	/*
	 * 	Computes the entropy value of the feature for discrete values of the features.
	 *  To compute entropy real values a copy of the feature is used whose real values are rounded
	 *  and it is scaled into the range [0, 255]
	 */
	public double computeEntropyDiscrete() {
		
		int start = 0, end = 255;
		
		double feat[] = null;
		if (this.featureType.equals(DescriptorType.SURF)) {
			// SURF features are discretized and converted into new range to compute entropy
			double oldMin = DescriptorType.getMinimumValue(this.featureType);
			double oldMax = DescriptorType.getMaximumValue(this.featureType);
			feat = modifyRange(this.feature, oldMin, oldMax, start, end);
		}
		else {
			feat = this.feature;
		}
		
		int length = feat.length;
		
		// Compute the hits of each value into its bin
		Map<Integer, Integer> hitMap= new HashMap<Integer, Integer>();
		
		// Fill hit map
		for (int i = 0; i < length; ++i) {
			
			double values = feat[i];
			int discrete = (int) Math.floor(values);
			
			if (!hitMap.containsKey(discrete)) {
				hitMap.put(discrete, 1);
			}
			else {
				int count = hitMap.get(discrete);
				hitMap.put(discrete, count + 1);
			}
		}
		
		// Compute entropy
		double sum = 0.0;
		for (int i = start; i <= end; ++i) {
			if (hitMap.containsKey(i)) {
				int hitTerm = hitMap.get(i);
				double hitNorm = (double) hitTerm / (double) length;
				double logHitNorm = Math.log(hitNorm) / log2;
				sum += hitNorm * logHitNorm;
			}
		}
		
		return sum * -1.0;
	}
	
	/*
	 * 	Computes the variance value of the feature for discrete values
	 * 	This variance assumes that all values have same probability of appeareance in a feature.
	 */
	public double computeVariance() {
		
		int length = this.feature.length;
		
		double mean = this.computeMean();
		double sum = 0.0;
		for (int i = 0; i < length; ++i) {
			sum += Math.pow(this.feature[i] - mean, 2);
		}
		
		// N - 1, variance of the sample
		return sum / (double) (length - 1);
	}
	
	/*
	 * 	Computes the mean of the feature
	 * 	@return Mean of the feature
	 */
	protected double computeMean() {
		double sum = 0.0;
		int length = this.feature.length;
		for (int i = 0; i < length; ++i) {
			sum += this.feature[i];
		}
		return sum / (double) length;
	}
	
	/*
	 * 	Computes the hamming distance between the feature and the input feature.
	 * 	@return Hamming distance between both features. Returns -1 if any of the sketches
	 * 		has not been computed or if size of blocks from both are different
	 *
	 */
	public int getHammingDistance(ImageFeature other) {
		FeatureSketch first = this.getSketch();
		FeatureSketch second = other.getSketch();
		
		if (first == null || second == null || 
				first.getHashes().length != second.getHashes().length) {
			return -1;
		}
		
		BitSet[] hashesFirst = first.getHashes();
		BitSet[] hashesSecond = second.getHashes();
		
		int sum = 0;
		for (int i = 0; i < hashesFirst.length; ++i) {
			sum += getHammingDistance(hashesFirst[i], hashesSecond[i]);
		}
		return sum;
	}
	
	
	/*
	 * 	@param first First vector
	 * 	@param second Second vector
	 * 	@return Returns the Hamming distance between the two boolean representations
	 */
	protected static int getHammingDistance(BitSet first, BitSet second) {
		// Hamming distance is equal to # of 1s after and 'XOR' 
		BitSet xor = first.$up(second);
		return xor.cardinality();
	}
	
	/*
	 * 	Quantization
	 */
			
	public void quantizeFeature(int blocks) throws InvalidArgumentException {
		
		int featureSize = DescriptorType.getSize(this.featureType);
		// Get rounded size so we make sure it always fits
		// Quantization takes 2 *  num_values_feature
		int bitsPerBlock = (int) Math.round((double)featureSize * 2/(double)blocks);
		
		if (bitsPerBlock > BLOCK_SIZE) {
			throw new InvalidArgumentException("Block too big: " + bitsPerBlock 
					+ ". Maximum size is :" + BLOCK_SIZE);
		}
		
		double[] intervals = DescriptorType.getCuts(this.featureType);
		
		BitSet[] sketches = new BitSet[blocks];
		// For each of the k bits selected
		for (int i = 0; i < featureSize; ++i) {
			
			double value = this.feature[i];
			boolean[] bits = quantize(intervals, value);
			
			for (int j = 0; j < bits.length; ++j) {
				
				// Compute hash value
				int position = (2 * i) + j;
				int block = (int) position / bitsPerBlock;
				if (position % bitsPerBlock == 0) {
					sketches[block] = new BitSet(bitsPerBlock);
				}

				if (bits[j]) {
					int blockPosition = (position - block * bitsPerBlock) + j;
					sketches[block].set(blockPosition);
				}
			}
		}
		
		this.sketch = new FeatureSketch(sketches);
	}
	
	/*
	 * 	Quantizes a a 
	 */
	public static boolean[] quantize(double[] intervals, double value) {
		
		if (value <= intervals[0]) {
			return new boolean[]{ false, false };
		} 
		else if (value > intervals[0] && value <= intervals[1]) {
			return new boolean[] { true, false };
		}
		else if (value > intervals[1] && value <= intervals[2]) {
			return new boolean[] { false, true };
		}
		else {
			return new boolean[] { true, true };
		}
	}
	
	private static final char SEPARATOR_CHAR_CMP = '_';
	
	@Override
    public int hashCode() {
		String first = null;
		try {
			first = this.sketch.getStringFromBlocksSeparated(SEPARATOR_CHAR_CMP);
		} catch (InvalidArgumentException e) {
			logger.error("Retrieved invalid block when comparign image features: " + e.getMessage());
		}
		String second = String.valueOf(imgId);
		String comb = first + "_" + second;
        return comb.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
       if (!(obj instanceof ImageFeature))
            return false;
        if (obj == this)
            return true;

        ImageFeature other = (ImageFeature) obj;
        try {
			return (other.getImgId() == imgId) 
					&& (other.getSketch().getStringFromBlocksSeparated(SEPARATOR_CHAR_CMP)
							.equals(sketch.getStringFromBlocksSeparated(SEPARATOR_CHAR_CMP)));
		} catch (InvalidArgumentException e) {
			return false;
		}
    }
	
	
}
