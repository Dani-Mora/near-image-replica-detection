package org.twitterReplica.model.replica;

import org.opencv.core.Mat;

public class GammaCorrectionReplica extends Replica {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4803510203480664697L;
	
	private double gamma;
	
	public GammaCorrectionReplica(Mat image, long id, long srcId, double gamma) {
		super(image, id, srcId);
		this.gamma = gamma;
	}
	
	public GammaCorrectionReplica(Mat image, long id, String path, long srcId, double gamma) {
		super(image, id, path, srcId);
		this.gamma = gamma;
	}

	public double getGamma() {
		return gamma;
	}

	public void setGamma(double gamma) {
		this.gamma = gamma;
	}

	@Override
	public String getExtraName() {
		return "_gamma_" + String.valueOf(this.gamma);
	}

	@Override
	public String getReplicaLabel() {
		return ReplicaType.Gamma.toString();
	}
	
	@Override
	public String[] getParameters() {
		return new String[]{ String.valueOf(this.getGamma()), null };
	}

	
	
}
