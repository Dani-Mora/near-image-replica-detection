package org.twitterReplica.model.replica;

import org.opencv.core.Mat;

public class TextAdditionReplica extends Replica {

	/**
	 * 
	 */
	private static final long serialVersionUID = -960111389636023371L;
	
	private int length;
	
	public TextAdditionReplica(Mat image, long id, long srcId, int length) {
		super(image, id, srcId);
		this.length = length;
	}
	
	public TextAdditionReplica(Mat image, long id, String path, long srcId, int length) {
		super(image, id, path, srcId);
		this.length = length;
	}

	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}

	@Override
	public String getExtraName() {
		return "_text_" + String.valueOf(length);
	}
	
	@Override
	public String getReplicaLabel() {
		return ReplicaType.Text.toString();
	}
	
	@Override
	public String[] getParameters() {
		return new String[]{ String.valueOf(this.getLength()), null };
	}

}
