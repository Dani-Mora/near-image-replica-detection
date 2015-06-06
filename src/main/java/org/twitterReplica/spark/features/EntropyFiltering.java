package org.twitterReplica.spark.features;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.twitterReplica.model.ImageFeature;
import org.twitterReplica.model.ImageInfo;

import scala.Tuple2;

public class EntropyFiltering implements Function<Tuple2<ImageInfo, ImageFeature>, Boolean> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7707998607239902194L;

	static Logger logger = Logger.getLogger(EntropyFiltering.class);
	
	private double thresh;
	
	public EntropyFiltering(double thresh) {
		this.thresh = thresh;
	}
	
	public Boolean call(Tuple2<ImageInfo, ImageFeature> feat) throws Exception {
		return feat._2.computeEntropyDiscrete() > this.thresh;
	}
	

}
