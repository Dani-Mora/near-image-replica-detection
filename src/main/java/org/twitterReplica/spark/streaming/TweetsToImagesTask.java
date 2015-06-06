package org.twitterReplica.spark.streaming;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.twitterReplica.model.ImageInfo;
import org.twitterReplica.model.providers.ProviderType;

import twitter4j.MediaEntity;
import twitter4j.Status;

public class TweetsToImagesTask implements FlatMapFunction<Iterator<Status>, ImageInfo> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2566973162686030928L;
	public static final String PHOTO_ENTITY = "photo";

	public Iterable<ImageInfo> call(Iterator<Status> t)
			throws Exception {
		List<ImageInfo> imgs = new ArrayList<ImageInfo>();
		
		while(t.hasNext()) {
			Status current = t.next();
			MediaEntity[] entities = current.getMediaEntities();
			for (MediaEntity m : entities) {
				if (m.getType().equals(PHOTO_ENTITY)) {
					System.out.println("Found image in tweet. Url: " + m.getMediaURL() + ", Id: " + m.getId() + " tweet: " + current.getId());
					imgs.add(new ImageInfo(m.getMediaURL(), m.getId(), 
							String.valueOf(current.getId()), ProviderType.TWITTER));
				}
			}
		}
		
		return imgs;
	}

}
