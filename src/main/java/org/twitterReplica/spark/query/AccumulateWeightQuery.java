package org.twitterReplica.spark.query;

import org.apache.spark.api.java.function.Function2;

public class AccumulateWeightQuery implements Function2<Long, Long, Long> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2761090605481257992L;

	public Long call(Long v1, Long v2) throws Exception {
		return v1 + v2;
	}

}
