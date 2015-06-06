package org.twitterReplica.spark.query;

import org.apache.spark.api.java.function.Function;
import org.twitterReplica.model.ImageMatch;

import scala.Tuple2;

public class WeightFiltering implements Function<Tuple2<ImageMatch, Long>, Boolean> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7635601531990254117L;
	private int weight;
	
	public WeightFiltering(int weight) {
		super();
		this.weight = weight;
	}

	@Override
	public Boolean call(Tuple2<ImageMatch, Long> v1) throws Exception {
		return v1._2() >= this.weight;
	}

}
