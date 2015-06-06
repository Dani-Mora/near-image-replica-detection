package org.twitterReplica.spark.features;

import org.apache.spark.api.java.function.Function;
import org.twitterReplica.model.ImageFeature;

public class LogScaleFunction implements Function<ImageFeature, ImageFeature> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4498668141598173494L;

	public ImageFeature call(ImageFeature t) throws Exception {
		t.logScale();
		return t;
	}

}
