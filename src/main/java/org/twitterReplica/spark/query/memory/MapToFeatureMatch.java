package org.twitterReplica.spark.query.memory;

import org.apache.spark.api.java.function.PairFunction;
import org.twitterReplica.model.ImageFeature;

import scala.Tuple2;

public class MapToFeatureMatch implements PairFunction<Tuple2<Tuple2<ImageFeature, ImageFeature>, Integer>,  ImageFeature, ImageFeature>  {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6931150838187720766L;

	public Tuple2<ImageFeature, ImageFeature> call(Tuple2<Tuple2<ImageFeature, ImageFeature>, Integer> t)
			throws Exception {
		return new Tuple2<ImageFeature, ImageFeature>(t._1._1, t._1._2);
	}

}
