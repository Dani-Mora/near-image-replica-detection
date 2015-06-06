package org.twitterReplica.model.providers;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.security.ProviderException;
import java.util.ArrayList;
import java.util.List;

import org.twitterReplica.exceptions.InitializationException;
import org.twitterReplica.exceptions.NotImageResourceException;
import org.twitterReplica.exceptions.ParseResourceException;
import org.twitterReplica.model.ImageInfo;
import org.twitterReplica.model.replica.ReplicaType;

import au.com.bytecode.opencsv.CSVReader;

public class DatasetImgProvider extends ResourceProvider<String[]> {

	private String queryPath;
	private char separator;
	private String datasetPath;
	private boolean end = false;
	
	private CSVReader reader;
	
	/*
	 * 	Creates an instance of the DatasetImgProvider
	 * 	@param datasetPath Path where images from the dataset are stored
	 * 	@param query File containing the image queries
	 * 	@param sep Separator in the query file
	 */
	public DatasetImgProvider(String datasetPath, String query, char sep) {
		this.datasetPath = datasetPath;
		this.queryPath = query;
		this.separator = sep;
	}
	
	@Override
	protected void initialize() throws InitializationException { 
		try {
			this.reader = new CSVReader(new FileReader(this.queryPath), this.separator);
		} catch (FileNotFoundException e) {
			 throw new InitializationException("Could not initialize reader: " + e.getMessage());
		}
	}

	@Override
	protected void startConnection() { }

	@Override
	protected boolean isConnectionDone()  {
		return this.end;
	}

	@Override
	protected String[] getNextResource() throws ProviderException {
		try {
			return this.reader.readNext();
		} catch (IOException e) {
			throw new ProviderException(e.getMessage());
		}
	}

	@Override
	protected List<ImageInfo> parseResource(String[] message)
			throws ParseResourceException, NotImageResourceException {
		
		List<ImageInfo> res = new ArrayList<ImageInfo>();
		
		if (message == null) {
			// Reached end of document, so end connection
			this.end = true;
			throw new ParseResourceException("Reached end of document");
		}
		else {
			
			ImageInfo im = null;
			if (message[5].equals("")) {
				// Not replica
				im = new ImageInfo(this.datasetPath + File.separatorChar + message[1], 
						Long.valueOf(message[0]), message[0], ProviderType.DISK);
			}
			else {
				// Replica
				im = new ImageInfo(this.datasetPath + File.separatorChar + message[1], 
						Long.valueOf(message[0]), message[0], ProviderType.DISK, 
						ReplicaType.valueOf(message[5]), Long.valueOf(message[8]));
			}
			
			res.add(im);
			return res;
		}
		
	}

	@Override
	public void disconnect() throws ProviderException {
		try {
			this.reader.close();
		} catch (IOException e) {
			throw new ProviderException(e.getMessage());
		}
	}

}
