package org.twitterReplica.core;

import org.apache.spark.api.java.JavaSparkContext;
import org.twitterReplica.exceptions.ConnectionException;
import org.twitterReplica.exceptions.DataException;
import org.twitterReplica.exceptions.InitializationException;
import org.twitterReplica.exceptions.InvalidArgumentException;
import org.twitterReplica.exceptions.NotFoundException;
import org.twitterReplica.model.DescriptorType;
import org.twitterReplica.model.PersistenceMode;
import org.twitterReplica.model.SketchFunction;

public abstract class ReplicaSystem implements IDetector {

	protected DescriptorParams descParams;
	protected FilteringParams filtParams;
	protected IndexingParams indParams;
	protected ReplicaConnection conn;
	
	protected IPersistence system;
	
	public DescriptorParams getDescriptorParams() {
		return descParams;
	}

	public FilteringParams getFilteringParams() {
		return filtParams;
	}

	public IndexingParams getIndexingParams() {
		return indParams;
	}

	public ReplicaSystem(PersistenceMode mode, ReplicaConnection conf) {
		super();
		conn = conf;
		system = mode.equals(PersistenceMode.MEMORY_ONLY) ? new MemoryPersistenceSystem()
			: new DiskPersistenceSystem();
	}
	
	@Override
	public void connect(ReplicaConnection params, JavaSparkContext spark) throws ConnectionException {
		// Cache detection parameters
		try{
			descParams = system.readDescriptorParams(params, spark);
			filtParams = system.readFilteringParams(params, spark);
			indParams = system.readIndexingParams(params, spark, DescriptorType.getSize(descParams.getDescriptorType()));
		} catch(DataException e) {
			throw new ConnectionException("Error connection to the replica system: " + e.getMessage());
		} catch(NotFoundException e) {
			throw new ConnectionException("Error reading replica paratemers: " + e.getMessage());
		}
		
		// Notify specific instance
		system.onConnected();
	}

	@Override
	public void initialize(ReplicaConnection cParams, DescriptorParams dParams, FilteringParams fParams, 
			int numTables, int W, int h, boolean dataBlockEnc, boolean compression, 
			int ttl) throws InvalidArgumentException, InitializationException {
		
		if (h >= numTables || h < 0) {
			throw new InvalidArgumentException("Hamming distance can never be greater or equal than"
					+ " the number of tables and must be positive");
		}
		
		if (numTables < 0) {
			throw new InvalidArgumentException("Number of tables can't be negative");
		}
		
		
		// Cache parameters
		descParams = dParams;
		filtParams = fParams;
		int featSize = DescriptorType.getSize(descParams.getDescriptorType());
		indParams = new IndexingParams(SketchFunction.getRandomLSHFunction(featSize, W), 
				numTables, h, dataBlockEnc, compression, ttl);
		
		try {
			// Restart data
			system.restartData(conn, indParams);
		} catch (DataException e) {
			throw new InitializationException("Error reseting data: " + e.getMessage());
		}
		
		try {
			// Persist parameters
			system.storeParameters(conn, descParams, indParams, filtParams);
		} catch (DataException e) {
			throw new InitializationException("Error persisting parameters: " + e.getMessage());
		}
	}
	
}
