package org.twitterReplica.spark.indexing.streaming;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.twitterReplica.core.IndexingParams;
import org.twitterReplica.core.ReplicaConnection;
import org.twitterReplica.model.ImageFeature;
import org.twitterReplica.model.ImageInfo;
import org.twitterReplica.spark.indexing.IndexImagesPartition;

public class IndexStreamingHBase implements Function<JavaPairRDD<ImageInfo, Iterable<ImageFeature>>, Void> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 355062519385827234L;

	private IndexingParams indParams;
	private ReplicaConnection conn;
	
	public IndexStreamingHBase(IndexingParams indParams, ReplicaConnection cParams) {
		this.indParams = indParams;
		this.conn = cParams;
	}
	
	public Void call(JavaPairRDD<ImageInfo, Iterable<ImageFeature>> v1) {
		v1.foreachPartition(new IndexImagesPartition(this.indParams, this.conn));
		return null;
	}

}
