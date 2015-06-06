package org.twitterReplica.spark.features;

import org.apache.spark.api.java.function.Function;
import org.twitterReplica.model.ImageFeature;
import org.twitterReplica.model.ImageInfo;

import scala.Tuple2;

public class VarianceFiltering implements Function<Tuple2<ImageInfo, ImageFeature>, Boolean> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7707998607239902194L;

	private double thresh;
	
	public VarianceFiltering(double thresh) {
		this.thresh = thresh;
	}
	
	public Boolean call(Tuple2<ImageInfo, ImageFeature> feat) throws Exception {
		return feat._2.computeVariance() > this.thresh;
	}
	

}
