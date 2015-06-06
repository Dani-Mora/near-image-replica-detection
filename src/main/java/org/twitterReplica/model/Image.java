package org.twitterReplica.model;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.opencv.core.Mat;
import org.opencv.core.MatOfKeyPoint;
import org.opencv.features2d.DescriptorExtractor;
import org.opencv.highgui.Highgui;
import org.twitterReplica.core.DescriptorParams;
import org.twitterReplica.improcessing.ImageTools;
import org.twitterReplica.model.keypoints.KeypDetectors;
import org.twitterReplica.model.keypoints.KeypointExtractor;
import org.twitterReplica.model.providers.ProviderType;
import org.twitterReplica.utils.ReplicaUtils;

public class Image extends ImageInfo {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3610883381357316194L;
	
	public static final double log2 = Math.log(2);
	
	private static final DescriptorExtractor SIFT_EXTRACTOR = DescriptorExtractor.create(DescriptorExtractor.SIFT);
	private static final DescriptorExtractor SURF_EXTRACTOR = DescriptorExtractor.create(DescriptorExtractor.SURF);
	private static final DescriptorExtractor ORB_EXTRACTOR = DescriptorExtractor.create(DescriptorExtractor.ORB);
	private static final DescriptorExtractor FREAK_EXTRACTOR = DescriptorExtractor.create(DescriptorExtractor.FREAK);
	private static final DescriptorExtractor BRISK_EXTRACTOR = DescriptorExtractor.create(DescriptorExtractor.BRISK);
	private static final DescriptorExtractor BRIEF_EXTRACTOR = DescriptorExtractor.create(DescriptorExtractor.BRIEF);
	
	private static final Integer GRAYSCALE_TYPE = 0;
	
	private Mat image;
	private Mat descriptor;
	
	public Image(Mat image, long id) {
		super(id);
		this.image = image;
	}

	public Image(long id, Mat image, String path) {
		super(path, id);
		this.image = image;
	}
	
	public Image(Mat img, String path, long id, String resId, ProviderType provider) {
		super(path, id, resId, provider);
		this.image = img;
	}

	public Mat getImage() {
		return image;
	}

	public void setImage(Mat image) {
		this.image = image;
	}
	
	public Mat getDescriptor() {
		return descriptor;
	}
	
	public List<Mat> getDescriptorAsList() {
		List<Mat> result = new ArrayList<Mat>();
		for(int i = 0; i < this.getDescriptor().size().height; ++i) {
			result.add(this.getDescriptor().row(i));
		}
		return result;
	}

	public void setDescriptor(Mat descriptor) {
		this.descriptor = descriptor;
	}

	public void toFile(String file) throws IOException {
		ReplicaUtils.matrixToFile(this.getImage(), file);
	}
	
	/*
	 * 	Computes the descriptor of the image given the specified parameters
	 * 	
	 * 	@param dType Type of descriptor to compute
	 * 	@param kType Type of interest point detector
	 * 	@param fType Type of filtering to apply to the features
	 * 	@param threshold If we want to apply any filtering, use this field to specify the threshold 
	 * 	@param side Size in pixels of largest side of the resized image we want to get the descriptor from
	 * 	@return Descriptor computed
	 * 
	 */
	public void computeDescriptor(DescriptorParams params) {
		this.computeDescriptor(params.getDescriptorType(), params.getKeypointType(), 
				params.getMaximumLargestSide());
	}
	
	/*
	 * 	Computes the descriptor of the image given the specified parameters
	 * 	
	 * 	@param dType Type of descriptor to compute
	 * 	@param kType Type of interest point detector
	 * 	@param side Size in pixels of largest side of the resized image we want to get the descriptor from
	 * 	@return Descriptor computed
	 * 
	 */
	public void computeDescriptor(DescriptorType dType, KeypDetectors kType, int side) {
		
		Mat img = this.image;
		
		// Transform to gray space if not already grayscale
		if (img.type() != GRAYSCALE_TYPE)
		{
			img = ImageTools.RGBtoGrayScale(this.getImage()); 
		}
				
		// Resize image so larger side is as long as input
		img = ImageTools.resizeImage(img, side);
		
		// Compute reference points
		MatOfKeyPoint points = KeypointExtractor.extractPoints(img, kType);
		
		Mat result = new Mat();
		
		if (dType.equals(DescriptorType.SIFT)) {
			result = compute(img, points, SIFT_EXTRACTOR);
		}
		else if (dType.equals(DescriptorType.ORB)) {
			result = compute(img, points, ORB_EXTRACTOR);
		}
		else if (dType.equals(DescriptorType.FREAK)) {
			result = compute(img, points, FREAK_EXTRACTOR);
		}
		else if (dType.equals(DescriptorType.BRISK)) {
			result = compute(img, points, BRISK_EXTRACTOR);
		}
		else if (dType.equals(DescriptorType.BRIEF)) {
			result = compute(img, points, BRIEF_EXTRACTOR);
		}
		else if (dType.equals(DescriptorType.SURF)) {
			result = compute(img, points, SURF_EXTRACTOR);
		}

		this.descriptor = result;
	}
	
	/*
	 * 	Computes the descriptor of the image for built-in OpenCV descriptors
	 * 
	 * 	@param imgID Identifier of the image
	 * 	@param img Source image
	 * 	@param points Matrix of key points of the source image
	 * 	@param extractor Type of descriptor to compute
	 * 	@param start Minimum value of the final descriptor
	 * 	@param end Maximum value of the final descriptor
	 * 	@param fType Type of filtering to apply
	 * 	@param thresh Threshold to use in case of filtering
	 *  @return Computed descriptor
	 */
	private static Mat compute(Mat img, MatOfKeyPoint points, DescriptorExtractor extractor) {
		// Compute descriptor
		Mat descriptor = new Mat();
		extractor.compute(img, points, descriptor);
		return descriptor;
	}
	
	public static Mat readImageFromPath(String filePath) throws IOException {
		if (ReplicaUtils.isHDFS(filePath)) {
			return ReplicaUtils.readFromHDFS(filePath);
		}
		else if (ReplicaUtils.isURL(filePath)) {
			return ReplicaUtils.getImageContentFromURL(new URL(filePath));
		}
		else {
			if (ReplicaUtils.fileExists(filePath)) {
				return Highgui.imread(filePath, Highgui.CV_LOAD_IMAGE_ANYCOLOR);
			}
			else {
				throw new IOException("Path '" + filePath + "' does not exist");
			}
		}
	}
	
}
