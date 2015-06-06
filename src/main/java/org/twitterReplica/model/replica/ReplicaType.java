package org.twitterReplica.model.replica;

import scala.Serializable;

public enum ReplicaType implements Serializable {

	ChannelS,
	ChannelV,
	Compressed,
	Gamma,
	Gaussian,
	Occluded,
	HFlipped,
	Resized,
	Rotated,
	Cropped_H,
	Cropped_V,
	Smoothed,
	Text;
	
}
