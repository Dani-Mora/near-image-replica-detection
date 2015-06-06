package org.twitterReplica.benchmark;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;
import org.opencv.core.Mat;
import org.opencv.core.MatOfDMatch;
import org.opencv.core.Size;
import org.opencv.features2d.DMatch;
import org.opencv.features2d.DescriptorMatcher;
import org.twitterReplica.core.DescriptorParams;
import org.twitterReplica.dataset.DatasetGenerator;
import org.twitterReplica.exceptions.InvalidArgumentException;
import org.twitterReplica.exceptions.TestException;
import org.twitterReplica.improcessing.ImageTools;
import org.twitterReplica.model.DescriptorType;
import org.twitterReplica.model.Image;
import org.twitterReplica.model.ImageFeature;
import org.twitterReplica.model.keypoints.KeypDetectors;
import org.twitterReplica.model.keypoints.KeypointExtractor;
import org.twitterReplica.model.replica.Replica;
import org.twitterReplica.utils.ReplicaUtils;

import au.com.bytecode.opencsv.CSVWriter;

import com.google.common.io.Files;

public class Measures {

	static Logger logger = Logger.getLogger(Measures.class.getName());
	
	public static final String[] ALLOWED_EXT = new String[]{ "jpeg", "jpg", "png"};
	public static final Size MIN_SIZE = new Size(75, 75);
	public static final Size MAX_SIZE = new Size(2048, 2048);
	public static final Integer MAX_LARGEST_SIDE = 350;
	
	/*
	 * 	Tests the available keypoints and stores the computing time for each
	 * 	@param folder Input folder containing images
	 * 	@param dstPath Destination path where to store the measures
	 */
	public static void testKeypointsCPU(String folder, String dstPath) throws TestException {
		
		// Initialize writer
		CSVWriter writer = null;
		
		try {
			writer = new CSVWriter(new FileWriter(dstPath));
			
			// Iterate through all images in folder
			File[] files = new File(folder).listFiles();
			for (File f : files) {
				if (ReplicaUtils.isExtAccepted(Files.getFileExtension(f.getPath()), ALLOWED_EXT)) {
					
					// Iterate through each type
					for (KeypDetectors type : KeypDetectors.values()) {
						// Compute keypoints and store CPU time
						double start = ReplicaUtils.getCPUTimeNow();
						Mat img = Image.readImageFromPath(f.getAbsolutePath());
						KeypointExtractor.extractPoints(img, type);
						double end = ReplicaUtils.getCPUTimeNow();
						double lapse = (end - start);
						writer.writeNext(new String[] { f.getAbsolutePath(), type.toString(), String.valueOf(lapse)});
					}
					
				}
			}
		} catch (IOException e) {
			throw new TestException("Test error: " + e.getMessage());
		} finally {
			if (writer != null) {
				try {
					writer.close();
				} catch (IOException e) {
					logger.warn("Error closing csv file: " + e.getMessage());
				}
			}
		}
	} 
	
	/*
	 * 	Saves the values of the features computed (entropy, variance and quality) using the 
	 * 	descriptor in the parameters
	 * 	@param folder Folder where to get pictures from
	 * 	@param dstPath CSV destination path
	 * 	@param params Parameters of the descriptor computation
	 */
	public static void testDescriptorValues(String folder, String dstPath, DescriptorParams params) throws TestException {
		
		// Initialize writer
		CSVWriter writer = null;
		
		try {
			writer = new CSVWriter(new FileWriter(dstPath));
			
			// Iterate through all images in folder
			File[] files = new File(folder).listFiles();
			for (File f : files) {
				if (ReplicaUtils.isExtAccepted(Files.getFileExtension(f.getPath()), ALLOWED_EXT)) {
					homographyTest(f.getPath(), writer, params);
				}
			}
		} catch (IOException e) {
			throw new TestException("Test error: " + e.getMessage());
		} finally {
			if (writer != null) {
				try {
					writer.close();
				} catch (IOException e) {
					logger.warn("Error closing csv file: " + e.getMessage());
				}
			}
		}
	}
	
	/*
	 * 	Good matches are considered following this tutorial: 
	 * 		http://docs.opencv.org/doc/tutorials/features2d/feature_homography/feature_homography.html#feature-homography
	 */
	private static void homographyTest(String path, CSVWriter writer, DescriptorParams params) 
			throws TestException, IOException {
		
		String fileName = Files.getNameWithoutExtension(path);
		
		// Compute descriptor of the source image
		Mat m = Image.readImageFromPath(path);
		Image img = new Image(m, -1);
		img.computeDescriptor(params);
		Mat imgDesc = img.getDescriptor();
		
		if (imgDesc.size().height == 0) {
			System.out.println("Image " + path + " computed no features for detector " + params.getKeypointType());
		}
		else {
			
			// Gather some parameters out of loop
			String keypointDetec = params.getKeypointType().toString();
			String descriptor = params.getDescriptorType().toString();
			String maxSide = String.valueOf(params.getMaximumLargestSide());
			
			// Generate replicas from image
			List<Replica> reps;
			try {
				reps = DatasetGenerator.generateReplicas(0, -1, path);
			} catch (IOException e) {
				throw new TestException("Error generating replicas: " + e.getMessage());
			} catch (InvalidArgumentException e) {
				throw new TestException("Error generating replicas: " + e.getMessage());
			}
			
			// Iterate over all replicas
			for (Replica r : reps) {
				
				String replicaType = r.getReplicaLabel();
				
				// Compute descriptor
				r.computeDescriptor(params);
				Mat repDesc = r.getDescriptor();
				
				if (repDesc.size().height != 0) {
					
					// Match descriptors
					DescriptorMatcher matcher = DescriptorMatcher.create(DescriptorMatcher.BRUTEFORCE);
					MatOfDMatch matches = new MatOfDMatch();
					matcher.match(imgDesc, repDesc, matches);
					
					// Iterate through matches to find maximum and minimum
					// Number of matches is the minimum of the length of the descriptors of both images matched
					List<DMatch> matchesList = matches.toList();
					double maxDist = 0; double minDist = 100;
					for( int i = 0; i < matchesList.size(); i++ ) { 
						double dist = matchesList.get(i).distance;
					    if( dist < minDist ) minDist = dist;
					    if( dist > maxDist ) maxDist = dist;
					}
					
					// Identify good matches
					for(int i = 0; i < matchesList.size(); i++){
						boolean isGood = (matchesList.get(i).distance) < (3 * minDist);
						int featureIndex = matchesList.get(i).trainIdx;
						String quality = isGood ? "good" : "normal";
						double[] featureValues = ImageTools.matToArray(repDesc, featureIndex);
						ImageFeature feature = new ImageFeature(-1, featureValues, params.getDescriptorType());
						String entropyStr = String.valueOf(feature.computeEntropyDiscrete());
						String varianceStr = String.valueOf(feature.computeVariance());
						String[] info = new String[]{ fileName, replicaType, String.valueOf(featureIndex), keypointDetec, descriptor, maxSide, 
								entropyStr, varianceStr, quality};
						// Write into CSV
						writer.writeNext(info);
					}
				}
				else {
					System.out.println("Replica " + r.getExtraName() + " from image " + path 
							+ " got no features for detector " + params.getKeypointType());
				}
			}
		}
	}
	
	/*
	 * 	Writes all values computed from the images in the given folder into the specific destination
	 * 	using the input parameters
	 * 	@param folder Folder where to get images from
	 * 	@param dstPath Destination path of the .csv file
	 * 	@param params Parameters of the descriptor computation
	 */
	public static void measureDescHistograms(String folder, String dstPath, DescriptorParams params) throws TestException {
		
		// Initialize writer
		CSVWriter writer = null;
		
		try {
			writer = new CSVWriter(new FileWriter(dstPath));
			
			// Iterate through all images in folder
			File[] files = new File(folder).listFiles();
			for (File f : files) {
				if (ReplicaUtils.isExtAccepted(Files.getFileExtension(f.getPath()), ALLOWED_EXT)) {
					
					// Compute descriptor of the source image
					Mat m = Image.readImageFromPath(f.getAbsolutePath());
					Image img = new Image(m, -1);
					img.computeDescriptor(params);
					Mat imgDesc = img.getDescriptor();
					
					// Write all values
					for (int i = 0; i < imgDesc.size().height; ++i) {
						for (int j = 0; j < imgDesc.size().width; ++j) {
							writer.writeNext(new String[] { String.valueOf(imgDesc.get(i, j)[0]) });
						}
					}
				}
			}
		} catch (IOException e) {
			throw new TestException("Test error: " + e.getMessage());
		} finally {
			if (writer != null) {
				try {
					writer.close();
				} catch (IOException e) {
					logger.warn("Error closing csv file: " + e.getMessage());
				}
			}
		}
	}
	
	/*
	 * 	Measures descriptor distributions and variance & entropy for all types of descriptors
	 * 	given the same keypoint detector
	 * 	@param srcFolder Image source folder
	 * 	@param dstFolder CSV destination folder
	 * 	@param keyp Keypoint detcetor to use
	 */
	public static void testAllDescriptors(String srcFolder, String dstFolder, KeypDetectors keyp) {
		//String folder = "/home/dani/PFC/Tests/Keypoints quality/Images";
		try {
			DescriptorParams params1 = new DescriptorParams(DescriptorType.SIFT, keyp, MAX_LARGEST_SIDE);
			testDescriptorValues(srcFolder, dstFolder + File.separatorChar + "descriptors_sift.csv", params1);
			DescriptorParams params2 = new DescriptorParams(DescriptorType.SURF, keyp, MAX_LARGEST_SIDE);
			testDescriptorValues(srcFolder, dstFolder + File.separatorChar +  "descriptors_surf.csv", params2);
			DescriptorParams params3 = new DescriptorParams(DescriptorType.ORB, keyp, MAX_LARGEST_SIDE);
			testDescriptorValues(srcFolder, dstFolder + File.separatorChar + "descriptors_orb.csv", params3);
			DescriptorParams params4 = new DescriptorParams(DescriptorType.FREAK,keyp, MAX_LARGEST_SIDE);
			testDescriptorValues(srcFolder, dstFolder + File.separatorChar + "descriptors_freak.csv", params4);
			DescriptorParams params5 = new DescriptorParams(DescriptorType.BRISK, keyp, MAX_LARGEST_SIDE);
			testDescriptorValues(srcFolder, dstFolder + File.separatorChar + "descriptors_brisk.csv", params5);
			DescriptorParams params6 = new DescriptorParams(DescriptorType.BRIEF, keyp, MAX_LARGEST_SIDE);
			testDescriptorValues(srcFolder, dstFolder + File.separatorChar + "descriptors_brief.csv", params6);
		} catch (TestException e) {
			System.out.println("Error: " + e.getMessage());
		}
	}
	
}
