package org.twitterReplica.spark.input;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class PropertiesReader implements PairFunction<String, String, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3530055699231748541L;
	private String separator;
	
	public PropertiesReader(String separator) {
		this.separator = separator;
	}

	public Tuple2<String, String> call(String t) throws Exception {
		String[] words = t.split(this.separator);
		return new Tuple2<String, String>(words[0], words[1]);
	}
	
}
