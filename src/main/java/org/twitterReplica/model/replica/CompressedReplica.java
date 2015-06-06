package org.twitterReplica.model.replica;

import org.opencv.core.Mat;

public class CompressedReplica extends Replica {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4095777944368066997L;
	
	private float quality;

	public CompressedReplica(Mat image, long id, long srcId, float quality) {
		super(image, id, srcId);
		this.quality = quality;
	}
	
	public CompressedReplica(Mat image, long id, String path, long srcId, float quality) {
		super(image, id, path, srcId);
		this.quality = quality;
	}

	public float getQuality() {
		return quality;
	}

	public void setQuality(float quality) {
		this.quality = quality;
	}

	@Override
	public String getExtraName() {
		return "_comp_" + String.valueOf(this.getQuality());
	}
	
	@Override
	public String getReplicaLabel() {
		return ReplicaType.Compressed.toString();
	}
	
	@Override
	public String[] getParameters() {
		return new String[]{ String.valueOf(this.getQuality()), null };
	}

}
