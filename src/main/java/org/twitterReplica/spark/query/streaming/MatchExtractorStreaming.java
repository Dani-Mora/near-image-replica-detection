package org.twitterReplica.spark.query.streaming;

import org.apache.spark.api.java.function.Function;
import org.twitterReplica.model.ImageFeature;
import org.twitterReplica.model.ImageMatch;

import scala.Tuple2;

public class MatchExtractorStreaming implements Function<Tuple2<ImageFeature, ImageFeature>, ImageMatch> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 14535L;

	public ImageMatch call(Tuple2<ImageFeature, ImageFeature> v1) throws Exception {
		return new ImageMatch(v1._1.getImgId(), v1._2.getImgId());
	}

}
