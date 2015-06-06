package org.twitterReplica.core;

import org.apache.spark.api.java.JavaSparkContext;
import org.twitterReplica.exceptions.ConnectionException;
import org.twitterReplica.exceptions.DataException;
import org.twitterReplica.exceptions.InitializationException;
import org.twitterReplica.exceptions.InvalidArgumentException;
import org.twitterReplica.model.ImageInfo;

public interface IDetector {

	/*
	 * 	Connects to the replica detection system
	 */
	public void connect(ReplicaConnection cParams, JavaSparkContext spark) throws ConnectionException;
	
	/*
	 * 	Restarts content and overwrites parameters of the replica detector
	 */
	public void initialize(ReplicaConnection cParams, DescriptorParams params, FilteringParams filtering, 
			int numTables, int W, int h, boolean dataBlockEnc, boolean compression, 
			int ttl) throws InvalidArgumentException, InitializationException;
	
	/*
	 * 	Getters
	 */
	
	public DescriptorParams getDescriptorParams();

	public FilteringParams getFilteringParams();

	public IndexingParams getIndexingParams();
	
	/*
	 * 	@param id Image identifier
	 * 	@return Information related to image with given identifier
	 */
	public ImageInfo getImage(long id) throws DataException;
	
}
