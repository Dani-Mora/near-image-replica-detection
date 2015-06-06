package org.twitterReplica.model.keypoints;

import org.opencv.core.Mat;
import org.opencv.core.MatOfKeyPoint;
import org.opencv.features2d.FeatureDetector;

public class KeypointExtractor {

	private static FeatureDetector BRISK_DETECTOR = FeatureDetector.create(FeatureDetector.BRISK);
	private static FeatureDetector ORB_DETECTOR = FeatureDetector.create(FeatureDetector.ORB);
	private static FeatureDetector SURF_DETECTOR = FeatureDetector.create(FeatureDetector.SURF);
	private static FeatureDetector HARRIS_DETECTOR = FeatureDetector.create(FeatureDetector.HARRIS);
	private static FeatureDetector SIFT_DETECTOR = FeatureDetector.create(FeatureDetector.SIFT);
	private static FeatureDetector MSER_DETECTOR = FeatureDetector.create(FeatureDetector.MSER);
	
	public static MatOfKeyPoint extractPoints(Mat img, KeypDetectors type) {
		
		MatOfKeyPoint result = new MatOfKeyPoint();
		
		switch(type) {
			case ORB:
				detectKeypoints(img, ORB_DETECTOR, result);
				break;
			case BRISK:
				detectKeypoints(img, BRISK_DETECTOR, result);
				break;
			case SURF:
				detectKeypoints(img, SURF_DETECTOR, result);
				break;
			case HARRIS:
				detectKeypoints(img, HARRIS_DETECTOR, result);
				break;
			case SIFT:
				detectKeypoints(img, SIFT_DETECTOR, result);
				break;
			case MSER:
				detectKeypoints(img, MSER_DETECTOR, result);
				break;
		}
		
		return result;
	}
	
	private static void detectKeypoints(Mat img, FeatureDetector detector, MatOfKeyPoint points) {
		detector.detect(img, points);
	}
	
}
