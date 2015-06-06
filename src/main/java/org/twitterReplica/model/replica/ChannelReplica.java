package org.twitterReplica.model.replica;

import org.opencv.core.Mat;

public class ChannelReplica extends Replica {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3309777775598534523L;
	
	private double percentage;
	private boolean satChannel;
	
	public ChannelReplica(Mat image, long id, long srcId, double perc, boolean saturationChannel) {
		super(image, id, srcId);
		this.percentage = perc;
		this.satChannel = saturationChannel;
	}
	
	public ChannelReplica(Mat image, String path, long id, long srcId, double perc, boolean saturationChannel) {
		super(image, id, path, srcId);
		this.percentage = perc;
		this.satChannel = saturationChannel;
	}
	
	public double getPercentage() {
		return percentage;
	}

	public void setPercentage(double percentage) {
		this.percentage = percentage;
	}

	public boolean isSaturationChannel() {
		return satChannel;
	}

	public void setSaturationChannel(boolean satChannel) {
		this.satChannel = satChannel;
	}

	public String getExtraName() {
		return "channel_" + (this.satChannel == true ? "s" : "v") + "_" + String.valueOf(this.percentage);
	}

	@Override
	public String getReplicaLabel() {
		return this.satChannel == true ? ReplicaType.ChannelS.toString() : ReplicaType.ChannelV.toString();
	}

	@Override
	public String[] getParameters() {
		return new String[]{ String.valueOf(this.getPercentage()), null };
	}

}
