package org.twitterReplica.jobs;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.opencv.core.Core;
import org.twitterReplica.dataset.DatasetGenerator;
import org.twitterReplica.exceptions.DatasetGenerationException;

/*
 * 	Generates the UPCReplica dataset given query folder where there are the following subfolders:
 * 		- src: Images to generate replicas from
 * 		- neg: Background images
 * 	Replicas from the 'src' folder (32 replicas per image) are generated in a new subfolder 're'.
 */

public class GenerateDataset {

	static Logger logger = Logger.getLogger(GenerateDataset.class);
	
	public static void main(String[] args) {
		
		// Load OpenCV
		System.loadLibrary( Core.NATIVE_LIBRARY_NAME );
		
		try {
			DatasetGenerator.generate(args[0]);
		} catch (IOException e) {
			System.out.println("Error generationg dataset: " + e.getMessage());
		} catch (DatasetGenerationException e) {
			System.out.println("Error generationg dataset: " + e.getMessage());
		}
	}
	
}
