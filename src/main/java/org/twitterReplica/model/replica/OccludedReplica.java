package org.twitterReplica.model.replica;

import org.opencv.core.Mat;

public class OccludedReplica extends Replica {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4074785342597395581L;
	
	private double radiusPercentage;
	private int numCircles;

	public OccludedReplica(Mat image, long id, long srcId, double radiusPercentage,
			int numCircles) {
		super(image, id, srcId);
		this.radiusPercentage = radiusPercentage;
		this.numCircles = numCircles;
	}
	
	public OccludedReplica(Mat image, long id, long srcId, String path, double radiusPercentage,
			int numCircles) {
		super(image, id, path, srcId);
		this.radiusPercentage = radiusPercentage;
		this.numCircles = numCircles;
	}

	public double getRadiusPercentage() {
		return radiusPercentage;
	}

	public void setRadiusPercentage(double radiusPercentage) {
		this.radiusPercentage = radiusPercentage;
	}

	public int getNumCircles() {
		return numCircles;
	}

	public void setNumCircles(int numCircles) {
		this.numCircles = numCircles;
	}

	@Override
	public String getExtraName() {
		return "_occlusion_" + "_radius_" + String.valueOf(this.getNumCircles()) + "_size_" 
				+ String.valueOf(this.getRadiusPercentage());
	}

	@Override
	public String getReplicaLabel() {
		return ReplicaType.Occluded.toString();
	}
	
	@Override
	public String[] getParameters() {
		return new String[]{ String.valueOf(this.getNumCircles()), String.valueOf(this.getRadiusPercentage()) };
	}

}
