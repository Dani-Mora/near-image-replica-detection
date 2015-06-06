package org.twitterReplica.spark.indexing.memory;

import org.apache.spark.api.java.function.PairFunction;
import org.twitterReplica.model.ImageFeature;
import org.twitterReplica.model.ImageInfo;

import scala.Tuple2;

public class FeatureToBlock implements PairFunction<Tuple2<ImageInfo, ImageFeature>, Integer, ImageFeature> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7499067305425207555L;

	private Integer tableIndex;
	
	public FeatureToBlock(Integer tableIndex) {
		this.tableIndex = tableIndex;
	}
	
	public Tuple2<Integer, ImageFeature> call(Tuple2<ImageInfo, ImageFeature> t) throws Exception {
		return new Tuple2<Integer, ImageFeature>(t._2().getSketch().getKey(this.tableIndex), t._2());
	}


}
