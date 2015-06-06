package org.twitterReplica.model.replica;

import org.opencv.core.Mat;

public class HFlippedReplica extends Replica {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2280504649619932966L;
	

	public HFlippedReplica(Mat image, long id, long srcId) {
		super(image, id, srcId);
	}
	
	public HFlippedReplica(Mat image, long id, String path, long srcId) {
		super(image, id, path, srcId);
	}

	@Override
	public String getExtraName() {
		return "_h_flipped";
	}
	
	@Override
	public String getReplicaLabel() {
		return ReplicaType.HFlipped.toString();
	}
	
	@Override
	public String[] getParameters() {
		return new String[]{ null, null };
	}

}
