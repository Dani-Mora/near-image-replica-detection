package org.twitterReplica.core;

import org.twitterReplica.model.DescriptorType;
import org.twitterReplica.model.keypoints.KeypDetectors;

import scala.Serializable;

public class DescriptorParams implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private DescriptorType descriptorType;
	private KeypDetectors keypointType;
	private int maximumLargestSide;
	
	public DescriptorParams() {
		super();
	}
	
	public DescriptorParams(DescriptorType descType, KeypDetectors keypointType, int maximumLargestSide) {
		this();
		this.descriptorType = descType;
		this.keypointType = keypointType;
		this.maximumLargestSide = maximumLargestSide;
	}

	public DescriptorType getDescriptorType() {
		return descriptorType;
	}

	public void setDescriptorType(DescriptorType descriptorType) {
		this.descriptorType = descriptorType;
	}

	public KeypDetectors getKeypointType() {
		return keypointType;
	}

	public void setKeypointType(KeypDetectors keypointType) {
		this.keypointType = keypointType;
	}

	public int getMaximumLargestSide() {
		return maximumLargestSide;
	}

	public void setMaximumLargestSide(int maximumLargestSide) {
		this.maximumLargestSide = maximumLargestSide;
	}

}
