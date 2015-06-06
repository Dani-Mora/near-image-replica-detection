package org.twitterReplica.spark.input;

import java.io.File;

import org.apache.spark.api.java.function.Function;
import org.twitterReplica.model.ImageInfo;
import org.twitterReplica.model.providers.ProviderType;

import com.google.common.io.Files;

public class FolderReader implements Function<String, ImageInfo> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6236530917839277852L;
	private String folderPath;
	
	public FolderReader(String path) {
		super();
		this.folderPath = path;
	}

	/*
	 * 	Reads images metadata from dataset references
	 * 	@param v1 Line query file
	 */
	public ImageInfo call(String v1) throws Exception {
		// This works in case path is numeric 
		Integer id = Integer.valueOf(Files.getNameWithoutExtension(v1));
		String path = this.folderPath + File.separatorChar + v1;
		return new ImageInfo(path, id, String.valueOf(id), ProviderType.DISK);
	}
	
}