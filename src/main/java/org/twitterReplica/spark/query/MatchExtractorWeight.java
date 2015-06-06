package org.twitterReplica.spark.query;

import org.apache.spark.api.java.function.PairFunction;
import org.twitterReplica.model.ImageFeature;
import org.twitterReplica.model.ImageMatch;

import scala.Tuple2;

public class MatchExtractorWeight implements 
	PairFunction<Tuple2<ImageFeature, ImageFeature>, ImageMatch, Long> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 14535L;

	public Tuple2<ImageMatch, Long> call(Tuple2<ImageFeature, ImageFeature> v1) throws Exception {
		return new Tuple2<ImageMatch, Long>(new ImageMatch(v1._1.getImgId(), v1._2.getImgId()), 1L);
	}

}
