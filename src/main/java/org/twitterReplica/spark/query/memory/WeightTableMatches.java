package org.twitterReplica.spark.query.memory;

import org.apache.spark.api.java.function.PairFunction;
import org.twitterReplica.model.ImageFeature;

import scala.Tuple2;

public class WeightTableMatches implements PairFunction<Tuple2<Integer, Tuple2<ImageFeature, ImageFeature>>, Tuple2<ImageFeature, ImageFeature>, Integer>  {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4173150677497077432L;

	public Tuple2<Tuple2<ImageFeature, ImageFeature>, Integer> call(Tuple2<Integer, Tuple2<ImageFeature, ImageFeature>> t)
			throws Exception {
		return new Tuple2<Tuple2<ImageFeature, ImageFeature>, Integer>(
				new Tuple2<ImageFeature, ImageFeature>(t._2._1, t._2._2), 1);
	}

}
