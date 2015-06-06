package org.twitterReplica.model.replica;

import org.opencv.core.Mat;
import org.twitterReplica.model.Image;

public abstract class Replica extends Image {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4913951020308759166L;
	
	public Replica(Mat image, long id, long srcId) {
		super(image, id);
		this.srcId = srcId;
	}
	
	public Replica(Mat image, long id, String path, long srcId) {
		super(id, image, path);
		this.srcId = srcId;
	}
	
	public Replica(Mat image, long id, String path, long srcId, ReplicaType rep) {
		super(id, image, path);
		this.srcId = srcId;
		this.replicaType = rep;
	}

	/*
	 * 	Get identifier of the source image
	 */
	public long getImageSrcId() {
		return this.srcId;
	}
	
	/*
	 * 	Get parameter type
	 */
	public ReplicaType getReplicaType() {
		return this.replicaType;
	}
	
	/*
	 * 	@return Name to represent the modifying operation
	 */
	public abstract String getExtraName();
	
	/*
	 * 	@return replica type label
	 */
	public abstract String getReplicaLabel();
	
	/*
	 * 	@return list of 2 parameters of the replica
	 */
	public abstract String[] getParameters();
	
}
