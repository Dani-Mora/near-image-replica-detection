package org.twitterReplica.spark.query.memory;

import org.apache.spark.api.java.function.Function;
import org.twitterReplica.model.ImageFeature;

import scala.Tuple2;

public class FilterTableMatches implements Function<Tuple2<Tuple2<ImageFeature, ImageFeature>, Integer>, Boolean>  {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2092263077804599108L;

	private int minimumMatches;
	
	public FilterTableMatches(int min) {
		this.minimumMatches = min;
	}
	
	public Boolean call(Tuple2<Tuple2<ImageFeature, ImageFeature>, Integer> v1) throws Exception {
		return v1._2 >= this.minimumMatches;
	}

}
