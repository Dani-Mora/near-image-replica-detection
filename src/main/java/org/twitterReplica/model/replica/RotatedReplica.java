package org.twitterReplica.model.replica;

import org.opencv.core.Mat;

public class RotatedReplica extends Replica {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1882682046752068226L;
	
	private float angle;

	public RotatedReplica(Mat image, long id, long srcId, float angle) {
		super(image, id, srcId);
		this.angle = angle;
	}
	
	public RotatedReplica(Mat image, long id, String path, long srcId, float angle) {
		super(image, id, path, srcId);
		this.angle = angle;
	}

	public float getAngle() {
		return angle;
	}

	public void setAngle(float angle) {
		this.angle = angle;
	}

	@Override
	public String getExtraName() {
		return "_rotate_" + String.valueOf(this.getAngle());
	}
	
	@Override
	public String getReplicaLabel() {
		return ReplicaType.Rotated.toString();
	}
	
	@Override
	public String[] getParameters() {
		return new String[]{ String.valueOf(this.getAngle()), null };
	}
	
}
