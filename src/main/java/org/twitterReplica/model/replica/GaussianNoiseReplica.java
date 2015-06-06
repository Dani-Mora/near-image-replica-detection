package org.twitterReplica.model.replica;

import org.opencv.core.Mat;

public class GaussianNoiseReplica extends Replica {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6110344820566994958L;
	
	private double mean;
	private double stdev;

	public GaussianNoiseReplica(Mat image, long id, long srcId, double mean, double variance) {
		super(image, id, srcId);
		this.mean = mean;
		this.stdev = variance;
	}
	
	public GaussianNoiseReplica(Mat image, long id, String path, long srcId, double mean, double variance) {
		super(image, id, path, srcId);
		this.mean = mean;
		this.stdev = variance;
	}

	public double getMean() {
		return mean;
	}

	public void setMean(double mean) {
		this.mean = mean;
	}

	public double getDeviation() {
		return stdev;
	}

	public void setDeviation(double variance) {
		this.stdev = variance;
	}

	@Override
	public String getExtraName() {
		return "_comp_" + "_mean_" + String.valueOf(this.getMean()) + "_stdev_" + String.valueOf(this.getDeviation());
	}

	@Override
	public String getReplicaLabel() {
		return ReplicaType.Gaussian.toString();
	}
	
	@Override
	public String[] getParameters() {
		return new String[]{ String.valueOf(this.getMean()), String.valueOf(this.getDeviation()) };
	}

}
