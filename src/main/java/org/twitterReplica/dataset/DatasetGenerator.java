package org.twitterReplica.dataset;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.opencv.core.Mat;
import org.opencv.core.Size;
import org.opencv.highgui.Highgui;
import org.twitterReplica.exceptions.DatasetGenerationException;
import org.twitterReplica.exceptions.InvalidArgumentException;
import org.twitterReplica.improcessing.ImageTools;
import org.twitterReplica.model.replica.ChannelReplica;
import org.twitterReplica.model.replica.CompressedReplica;
import org.twitterReplica.model.replica.CroppedReplica;
import org.twitterReplica.model.replica.GammaCorrectionReplica;
import org.twitterReplica.model.replica.GaussianNoiseReplica;
import org.twitterReplica.model.replica.HFlippedReplica;
import org.twitterReplica.model.replica.OccludedReplica;
import org.twitterReplica.model.replica.Replica;
import org.twitterReplica.model.replica.ResizedReplica;
import org.twitterReplica.model.replica.RotatedReplica;
import org.twitterReplica.model.replica.SmoothedReplica;
import org.twitterReplica.model.replica.TextAdditionReplica;
import org.twitterReplica.utils.ReplicaUtils;

import au.com.bytecode.opencsv.CSVWriter;

import com.google.common.io.Files;

public class DatasetGenerator {

	static Logger logger = Logger.getLogger(DatasetGenerator.class.getName());
	
	private static final String SOURCE_SUFFIX = "src";
	private static final String NEG_SUFFIX = "neg";
	private static final String REP_SUFFIX = "rep";
	private static final String DESC_NAME = "descriptor.csv";
	
	/*
	 * 	Generates the corresponding data set from the given folder. In this source folder there must be:
	 * 		- 'src' folder: where all the images which we take replicas from are located
	 * 		- 'neg' folder: where all background/negative images are located
	 * 	 
	 * 	A file called "descriptor.csv" will be created with the information about the elements of the data set.
	 * 	A folder (inside the given folder) called 'rep' will be created containing all the replicas from the source images.
	 * 	@param srcFolder Source folder
	 */
	public static void generate(String srcFolder) throws IOException, DatasetGenerationException {
		
		String srcPath = srcFolder + File.separatorChar + SOURCE_SUFFIX;
		String repPath = srcFolder + File.separatorChar + REP_SUFFIX;
		String negPath = srcFolder + File.separatorChar + NEG_SUFFIX;
		String dscPath = srcFolder + File.separatorChar + DESC_NAME;
		
		// Check paths
		if (!ReplicaUtils.folderExists(srcPath)) {
			throw new DatasetGenerationException("There is no '" + srcPath + "' containing the source images");
		}
		
		if (!ReplicaUtils.folderExists(negPath)) {
			throw new DatasetGenerationException("There is no '" + negPath + "' containing the background images");
		}
		
		// Creation of the replica folder
		ReplicaUtils.createFolder(repPath);
		
		// Initialize csv file
		CSVWriter writer = new CSVWriter(new FileWriter(dscPath));
		// Add header
		writer.writeNext(new String[]{"Image id", "Path", "Extension", "Height", "Width", "Type of Replica", "Replica param 1", "Replica param 2", "Replica of"});

		// Generate replicas and a copy of all reference images
		long count = 0;
		File refFolder = new File(srcPath);
		File refs[] = refFolder.listFiles();
		for (File f : refs) {
			
			// Store and increase id
			String relFilePath = SOURCE_SUFFIX + File.separatorChar + f.getName();
			long srcId = count++;
			String ext = imageToCSV(writer, srcId, f.getAbsolutePath(), relFilePath);
			
			// Store source image name
			String originalName = Files.getNameWithoutExtension(f.getName());
			
			// Generate replicas for the image
			List<Replica> imageReps;
			try {
				imageReps = generateReplicas(count, srcId, f.getPath());
			} catch (InvalidArgumentException e) {
				throw new DatasetGenerationException("Could not generate all replicas: " + e.getMessage());
			}
			
			// Store replicas metadata
			for (Replica r : imageReps) {
				String replicaPathAbs = repPath + File.separatorChar + originalName + r.getExtraName() + "." + ext;
				// Set path
				r.setPath(REP_SUFFIX + File.separatorChar + originalName + r.getExtraName() + "." + ext);
				// Write CSV
				replicaToCSV(writer, r, ext);
				// Store replica
				ReplicaUtils.matrixToFile(r.getImage(), replicaPathAbs);
				// Increase counter
				++count;
			}
		}
		
		// Store the non-reference images
		File nonRefFolder = new File(negPath);
		File nonRefs[] = nonRefFolder.listFiles();
		for (File f : nonRefs) {
			// Store and increase id
			String relFilePath = NEG_SUFFIX + File.separatorChar + f.getName();
			String fullPath = srcFolder + File.separatorChar + relFilePath;
			imageToCSV(writer, new Long(count), fullPath, relFilePath);
			count++;
		}
		
		writer.close();
	}
	
	/*
	 * 	Stores a non replica image into a CSV row
	 * 	@param writer CSV writer
	 * 	@param id Identifier of the image
	 * 	@param full Full path to image
	 * 	@param path path Relative path where the image is stored inside the dataset
	 * 	@return Extension of the image stored
	 */
	protected static String imageToCSV(CSVWriter writer, long id, String full, String path) {
		String ext = Files.getFileExtension(path);
		Mat src = Highgui.imread(full, Highgui.CV_LOAD_IMAGE_ANYCOLOR);
		writer.writeNext(new String[]{String.valueOf(id), path, ext,
				String.valueOf(src.size().height), String.valueOf(src.size().width), 
				null, null, null, null});
		return ext;
	}
	
	/*
	 * 	Stores a replica image into a CSV row
	 * 	@param writer CSV writer
	 * 	@param id Identifier of the image
	 * 	@return Extension of the image stored
	 */
	protected static void replicaToCSV(CSVWriter writer, Replica rep, String extension) {
		writer.writeNext(new String[]{ String.valueOf(rep.getId()), rep.getPath(), extension, 
				String.valueOf(rep.getImage().size().height), String.valueOf(rep.getImage().size().width), 
				rep.getReplicaLabel(), rep.getParameters()[0], rep.getParameters()[1], 
				String.valueOf(rep.getImageSrcId())} );
		
	}
	
	/*
	 * 	Prepares the structure to create a data set from a given folder. It extracts the number of given images from the
	 * 	source folder to create replicas from and the rest are assumed to be negative (background) images.
	 * 	Images are filtered by size and extension. Eliminates exact duplicates from the folder.
	 * 
	 * 	@param path Folder of the source of the images
	 * 	@param srcNum Number of images from the folder that will be used to create replicas from
	 * 	@param minSize Minimum accepted size
	 * 	@param maxSize Maximum accepted size
	 * 	@param allowedExtensions Extensions that we only accept
	 * 	@param dest Path where to store the data set generated
	 */
	public static void prepareFolders(String path, long srcNum, Size minSize, Size maxSize, 
			String[] allowedExtensions, String dest) throws IOException {
		
		// Check source folder
		File folder = new File(path);
		if (!folder.isDirectory()) {
			throw new IOException("The directory " + path + " does not exist");
		}
		
		// Create destination folder
		ReplicaUtils.createFolder(dest);
		
		// Check exact duplicates
		String srcFolder = dest + File.separatorChar + SOURCE_SUFFIX;
		String negFolder = dest + File.separatorChar + NEG_SUFFIX;
		
		// Generate destination folder
		ReplicaUtils.createFolder(dest);
		// Generate 'src' folder
		ReplicaUtils.createFolder(srcFolder);
		// Generate 'neg' folder
		ReplicaUtils.createFolder(negFolder);
		
		// Distribute files
		long count = 0;
		
		File files[] = folder.listFiles();
		for (File f : files) {
			Size imgSize = Highgui.imread(f.getPath()).size();
			boolean extOK = ReplicaUtils.isExtAccepted(Files.getFileExtension(f.getPath()), allowedExtensions);
			boolean sizeOK = ReplicaUtils.isSizeAccepted(imgSize, minSize, maxSize); 
			
			if (extOK && sizeOK) {
				
				// Valid image
				
				String destFolder = null;
				
				if (count < srcNum) {
					// Image goes to 'src'
					destFolder = srcFolder;
					++count;
				}
				else {
					// Image goes to 'neg'
					destFolder = negFolder;
				}
				
				// Copy file
				Files.copy(f, new File(destFolder + File.separatorChar + f.getName()));
			}
		}
	}
	
	/*
	 * 	Fix issues before creating a folder into a data set:
	 * 		- Removes exact duplicates
	 * 
	 * 	@param path Path of the folder to check
	 */
	public static void eliminateDuplicates(String path) throws IOException {
		
		// Check source folder
		File folder = new File(path);
		File files[] = folder.listFiles();

		// To delete vector
		boolean duplicates[] = new boolean[files.length];
		
		// Search for duplicates
		for (int i = files.length - 1; i >= 0; i--) {
			
			if (duplicates[i] == false) {
				
				// Check only if file was not already marked as a duplicate
				File first = files[i];
	
				for (int j = i - 1; j >= 0; j--) {
					File second = files[j];
					
					if (first.length() == second.length()) {
						
						// Check if matrices are equal
						boolean mEqual = ReplicaUtils.areMatricesEqual(Highgui.imread(first.getPath()), 
								Highgui.imread(second.getPath()));
						
						if (mEqual) {
							duplicates[j] = true;
						}
					}
				}
			}
		}
		
		for (int i = 0; i < files.length; ++i) {
			if (duplicates[i]) {
				if (files[i].delete()) {
					logger.info("Deleted duplicate '" + files[i].getPath() + "'");
				}
				else {
					logger.warn("Found duplicate '" + files[i].getPath() + "' but could not delete it");
				}
			}
		}
	}
	
	/*
	 *	Rename the files of inside the folder so each file has a unique name
	 * 	@param path Path of the folder to rename
	 */
	public static void renameFiles(String path) throws IOException {
		
		// Check source folder
		File folder = new File(path);
		if (!folder.isDirectory()) {
			throw new IOException("The directory " + path + " does not exist");
		}
		
		long count = 0;
		
		// Rename all files
		for (File f : folder.listFiles()) {
			String ext = Files.getFileExtension(f.getPath());
			String newPath = path + File.separatorChar + String.valueOf(count) + "." + ext;
			f.renameTo(new File(newPath));
			++count;
		}
	}
	
	/*
	 * 	Generates a set of "attacked" versions of the original image, including:
	 * 	rotation, cropping, compression, Gaussian noise addition, resizing, 
	 * 	average filtering, gray level conversion, S and V channel modifications and
	 * 	gamma correction.
	 * 	
	 * 	@param id First available id to assign
	 * 	@param srcId Source image identifier
	 * 	@param fileName File path to the source image
	 * 	@return List of replicas
	 */
	public static List<Replica> generateReplicas(long id, long srcId, String fileName) throws InvalidArgumentException, IOException {
		
		List<Replica> replicas = new ArrayList<Replica>();

		// Read image
		Mat src = Highgui.imread(fileName, Highgui.CV_LOAD_IMAGE_COLOR);
		
		// Rotation
		addReplicaToList(replicas, new RotatedReplica(ImageTools.rotate(src, 30), id++, srcId, 30));
		addReplicaToList(replicas, new RotatedReplica(ImageTools.rotate(src, 90), id++, srcId, 90));
		addReplicaToList(replicas, new RotatedReplica(ImageTools.rotate(src, 180), id++, srcId, 180));
		addReplicaToList(replicas, new RotatedReplica(ImageTools.rotate(src, 270), id++, srcId, 270));

		// Horizontal cropping
		addReplicaToList(replicas, new CroppedReplica(ImageTools.cropImageH(src, 0.25), id++, srcId, 0.25, true));
		addReplicaToList(replicas, new CroppedReplica(ImageTools.cropImageH(src, 0.50), id++, srcId, 0.50, true));
		
		// Vertical cropping
		addReplicaToList(replicas, new CroppedReplica(ImageTools.cropImageV(src, 0.25), id++, srcId,  0.25, false));
		addReplicaToList(replicas, new CroppedReplica(ImageTools.cropImageV(src, 0.50), id++, srcId, 0.50, false));
		
		// Compression
		addReplicaToList(replicas, new CompressedReplica(ImageTools.getJPEGCompressedImage(src, 0.1f), id++, srcId, 0.1f));
		addReplicaToList(replicas, new CompressedReplica(ImageTools.getJPEGCompressedImage(src, 0.3f), id++, srcId, 0.3f));
		addReplicaToList(replicas, new CompressedReplica(ImageTools.getJPEGCompressedImage(src, 0.5f), id++, srcId, 0.5f));
		addReplicaToList(replicas, new CompressedReplica(ImageTools.getJPEGCompressedImage(src, 0.7f), id++, srcId, 0.7f));
		
		// Gaussian noise addition
		addReplicaToList(replicas,new GaussianNoiseReplica(ImageTools.addAdditiveGaussianNoise(src, 0.0, 0.1), id++, srcId, 0.0, 0.1));
		addReplicaToList(replicas,new GaussianNoiseReplica(ImageTools.addAdditiveGaussianNoise(src, 0.5, 0.1), id++, srcId, 0.0, 0.25));
		
		// Resizing
		addReplicaToList(replicas,new ResizedReplica(ImageTools.resizeImage(src, 0.5), id++, srcId, 0.5));
		addReplicaToList(replicas,new ResizedReplica(ImageTools.resizeImage(src, 0.8), id++, srcId, 0.8));
		addReplicaToList(replicas,new ResizedReplica(ImageTools.resizeImage(src, 1.2), id++, srcId, 1.2));
		addReplicaToList(replicas,new ResizedReplica(ImageTools.resizeImage(src, 1.5), id++, srcId, 1.5));
		
		// Averaging filter
		addReplicaToList(replicas,new SmoothedReplica(ImageTools.smoothImage(src, 2), id++, srcId, 2));
		addReplicaToList(replicas,new SmoothedReplica(ImageTools.smoothImage(src, 3), id++, srcId, 3));
		
		// Gamma correction
		addReplicaToList(replicas,new GammaCorrectionReplica(ImageTools.applyGammaCorrection(src, 0.25), id++, srcId, 0.25));
		addReplicaToList(replicas,new GammaCorrectionReplica(ImageTools.applyGammaCorrection(src, 0.6), id++, srcId, 0.6));
		addReplicaToList(replicas,new GammaCorrectionReplica(ImageTools.applyGammaCorrection(src, 1.5), id++, srcId, 1.5));
		addReplicaToList(replicas,new GammaCorrectionReplica(ImageTools.applyGammaCorrection(src, 1.8), id++, srcId, 1.8));
		
		// HSV channel modification
		addReplicaToList(replicas,new ChannelReplica(ImageTools.modifySChannel(src, 0.10), id++, srcId, 0.10, true));
		addReplicaToList(replicas,new ChannelReplica(ImageTools.modifySChannel(src, -0.10), id++, srcId, -0.10, true));
		addReplicaToList(replicas,new ChannelReplica(ImageTools.modifyVChannel(src, -0.10), id++, srcId, -0.10, false));
		addReplicaToList(replicas,new ChannelReplica(ImageTools.modifyVChannel(src, 0.10), id++, srcId, 0.10, false));
		
		// Horizontal flipping
		addReplicaToList(replicas,new HFlippedReplica(ImageTools.horizontalFlipping(src), id++, srcId));
		
		// Text addition
		addReplicaToList(replicas,new TextAdditionReplica(ImageTools.addText(src, 15), id++, srcId, 15));
		
		// Occlusion
		addReplicaToList(replicas,new OccludedReplica(ImageTools.occludeImage(src, 3, 0.10), id++, srcId, 0.10, 3));
		addReplicaToList(replicas,new OccludedReplica(ImageTools.occludeImage(src, 2, 0.15), id++, srcId, 0.15, 2));
		
		return replicas;
	}
	
	/*
	 * 	Adds the replica into the given list and increases the id
	 */
	protected static void addReplicaToList(List<Replica> list, Replica rep) {
		list.add(rep);
	}
	
}
