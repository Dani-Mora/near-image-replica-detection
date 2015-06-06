package org.twitterReplica.model.replica;

import org.opencv.core.Mat;

public class SmoothedReplica extends Replica {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5945803288427857109L;
	
	private int kernelSize;

	public SmoothedReplica(Mat image, long id, long srcId, int kernelSize) {
		super(image, id, srcId);
		this.kernelSize = kernelSize;
	}
	
	public SmoothedReplica(Mat image, long id, String path, long srcId, int kernelSize) {
		super(image, id, path, srcId);
		this.kernelSize = kernelSize;
	}

	public int getKernelSize() {
		return kernelSize;
	}

	public void setKernelSize(int kernelSize) {
		this.kernelSize = kernelSize;
	}

	@Override
	public String getExtraName() {
		return "_smoothed_kernel_" + String.valueOf(this.getKernelSize());
	}
	
	@Override
	public String getReplicaLabel() {
		return ReplicaType.Smoothed.toString();
	}
	
	@Override
	public String[] getParameters() {
		return new String[]{ String.valueOf(this.getKernelSize()), null };
	}
	
}
