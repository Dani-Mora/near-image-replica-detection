package org.twitterReplica.model.replica;

import org.opencv.core.Mat;

public class CroppedReplica extends Replica {

	/**
	 * 
	 */
	private static final long serialVersionUID = -9134747828403442804L;
	
	private double cropped;
	private boolean horizontal;

	public CroppedReplica(Mat image, long id, long srcId,	double cropped, boolean horizontal) {
		super(image, id, srcId);
		this.cropped = cropped;
		this.horizontal = horizontal;
	}
	
	public CroppedReplica(Mat image, long id, String path, long srcId,	double cropped, boolean horizontal) {
		super(image, id, path, srcId);
		this.cropped = cropped;
		this.horizontal = horizontal;
	}

	public double getCroppedPercentage() {
		return cropped;
	}
	
	public boolean isHorizontal() {
		return this.horizontal;
	}

	@Override
	public String getExtraName() {
		return "_cropped_" + (this.isHorizontal() ? "h_" : "v_") + String.valueOf(this.getCroppedPercentage());
	}
	
	@Override
	public String getReplicaLabel() {
		return this.isHorizontal() ? ReplicaType.Cropped_H.toString() : ReplicaType.Cropped_V.toString();
	}
	
	@Override
	public String[] getParameters() {
		return new String[]{ String.valueOf(this.getCroppedPercentage()), null };
	}
	
}
