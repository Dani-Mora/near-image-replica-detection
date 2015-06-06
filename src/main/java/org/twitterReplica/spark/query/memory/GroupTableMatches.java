package org.twitterReplica.spark.query.memory;

import org.apache.spark.api.java.function.Function2;

public class GroupTableMatches implements Function2<Integer, Integer, Integer>  {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6527379494106565976L;

	public Integer call(Integer v1, Integer v2) throws Exception {
		return v1 + v2;
	}

}
