package org.twitterReplica.spark.input;

import java.io.File;

import org.apache.spark.api.java.function.Function;
import org.twitterReplica.model.ImageInfo;
import org.twitterReplica.model.providers.ProviderType;

public class DescriptorDiskReader implements Function<String, ImageInfo> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6236530917839277852L;
	private String datasetSrc;
	
	public DescriptorDiskReader(String datasetSrc) {
		super();
		this.datasetSrc = datasetSrc;
	}

	/*
	 * 	Reads images metadata from dataset references
	 * 	@param v1 Line query file
	 */
	public ImageInfo call(String v1) throws Exception {
		String[] pair = v1.split(",");
		long id;
		// Read id
		if (pair[0].charAt(0) == '"') {
			id = Long.valueOf(readBetweenQuotes(pair[0]));
		}
		else {
			id = Long.valueOf(pair[0]);
		}
		String path = readBetweenQuotes(pair[1]);
		return new ImageInfo(this.datasetSrc + File.separatorChar + path, id, String.valueOf(id), ProviderType.DISK);
	}
	
	protected static String readBetweenQuotes(String str) {
		return str.substring(1, str.length() - 1);
	}

}