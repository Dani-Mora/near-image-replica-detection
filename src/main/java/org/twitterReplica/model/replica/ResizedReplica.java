package org.twitterReplica.model.replica;

import org.opencv.core.Mat;

public class ResizedReplica extends Replica {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7221849734679538631L;
	
	private double scaleFactor;

	public ResizedReplica(Mat image, long id, long srcId, double scaleFactor) {
		super(image, id, srcId);
		this.scaleFactor = scaleFactor;
	}
	
	public ResizedReplica(Mat image, long id, long srcId, String path, double scaleFactor) {
		super(image, id, path, srcId);
		this.scaleFactor = scaleFactor;
	}

	public double getScaleFactor() {
		return scaleFactor;
	}

	public void setScaleFactor(double scaleFactor) {
		this.scaleFactor = scaleFactor;
	}

	public String getExtraName() {
		return "_resize_" + String.valueOf(this.getScaleFactor()); 
	}
	
	@Override
	public String getReplicaLabel() {
		return ReplicaType.Resized.toString();
	}
	
	@Override
	public String[] getParameters() {
		return new String[]{ String.valueOf(this.getScaleFactor()), null };
	}

}
