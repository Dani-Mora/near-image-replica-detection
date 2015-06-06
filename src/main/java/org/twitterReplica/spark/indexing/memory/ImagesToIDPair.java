package org.twitterReplica.spark.indexing.memory;

import org.apache.spark.api.java.function.PairFunction;
import org.twitterReplica.model.ImageInfo;

import scala.Tuple2;

public class ImagesToIDPair implements PairFunction<ImageInfo, Long, ImageInfo>  {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8671836121394690800L;

	@Override
	public Tuple2<Long, ImageInfo> call(ImageInfo t) throws Exception {
		return new Tuple2<Long, ImageInfo>(t.getId(), t);
	}

}
