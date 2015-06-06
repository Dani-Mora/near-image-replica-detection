package org.twitterReplica.spark.query;

import org.apache.spark.api.java.function.Function;
import org.twitterReplica.model.ImageFeature;

import scala.Tuple2;

public class HammingFiltering implements Function<Tuple2<ImageFeature, ImageFeature>, Boolean> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1317780873008680629L;

	private int hammingDistance;
	
	public HammingFiltering(int hammingDistance) {
		super();
		this.hammingDistance = hammingDistance;
	}

	public Boolean call(Tuple2<ImageFeature, ImageFeature> v1) throws Exception {
		return (v1._1.getHammingDistance(v1._2) <= this.hammingDistance);
	}

}