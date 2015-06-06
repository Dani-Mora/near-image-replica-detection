package org.twitterReplica.improcessing;

import java.awt.image.RenderedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.plugins.jpeg.JPEGImageWriteParam;
import javax.imageio.stream.ImageOutputStream;

import org.opencv.core.Core;
import org.opencv.core.CvType;
import org.opencv.core.Mat;
import org.opencv.core.MatOfFloat;
import org.opencv.core.MatOfInt;
import org.opencv.core.Point;
import org.opencv.core.Rect;
import org.opencv.core.Scalar;
import org.opencv.core.Size;
import org.opencv.imgproc.Imgproc;
import org.twitterReplica.exceptions.InvalidArgumentException;
import org.twitterReplica.utils.ReplicaUtils;

public class ImageTools {

	/*
	 * 	Converts a RGB image into a gray scale one
	 * 	@param source RGB input image
	 * 	@return Gray scale image converted from the original one
	 */
	public static Mat RGBtoGrayScale(Mat source) {
		Mat result = new Mat();
		Imgproc.cvtColor(source, result, Imgproc.COLOR_RGB2GRAY);
		return result;
	}
	
	/*
	 * 	Converts an RGB image into a HSV one
	 * 	@param source RGB input image
	 * 	@return HSV image converted from the original one
	 */
	public static Mat RGBtoHSV(Mat source) {
		Mat result = new Mat();
		Imgproc.cvtColor(source, result, Imgproc.COLOR_RGB2HSV);
		return result;
	}
	
	/*
	 * 	Converts an HSV image into a gray scale one
	 * 	@param source HSV input image
	 * 	@return Gray scale version of the input HSV image
	 */
	public static Mat HSVtoGrayScale(Mat source) {
		Mat result = new Mat();
		Imgproc.cvtColor(source, result, Imgproc.COLOR_HSV2RGB);
		Imgproc.cvtColor(result, result, Imgproc.COLOR_RGB2GRAY);
		return result;
	}
	
	/*
	 * 	@param source Original image
	 * 	@param height Desired height
	 * 	@param width Desired width
	 * 	@return Result of resizing original image to the specified height and width
	 */
	public static Mat resizeImage(Mat source, int height, int width) {
		Mat dest = new Mat();
		Imgproc.resize(source, dest, new Size(width, height));
		return dest;
	}
	
	/*
	 * 	@param maxSide Longest size of the largest side of the image
	 * 	@return Resizes an image so the largest side of it has the specified size
	 */
	public static Mat resizeImage(Mat source, int maxSide) {
		
		int oldWidth = (int) source.size().width;
		int oldHeight = (int) source.size().height;
		
		if (maxSide >= oldWidth && maxSide >= oldHeight) {
			// No need to resize, the picture is already within the limits
			return source;
		}
		
		// One or both of the sides are larger than the maximum
		Mat dest = new Mat();
		int newWidth, newHeight;
		double alpha;
		
		if (oldHeight > oldWidth) {
			// Height is larger than the maximum, need to limit it
			newHeight = maxSide;
			alpha = (double) oldHeight / (double) newHeight;
			newWidth = (int) Math.round(oldWidth / alpha);
		}
		else {
			// Width is larger or equal to height
			newWidth = maxSide;
			alpha = (double) oldWidth / (double) newWidth;
			newHeight = (int) Math.round(oldHeight / alpha);
		}
			
		Imgproc.resize(source, dest, new Size(newWidth, newHeight));
		return dest;
	}
	
	/*
	 * 	@param srcPoint Point to rotate
	 * 	@param center Center of rotation
	 * 	@param angle Angle to rotate (counter clock-wise) in degrees
	 * 	@return Rotated point coordinates
	 */
	public static Point rotatePoint(Point srcPoint, Point center, float angle) {
		// Angle to radiants
		double radiants = Math.toRadians(angle);
		// Prepare point to rotate
		Point relativePoint = new Point(srcPoint.x - center.x, srcPoint.y - center.y);
		double cos = Math.cos(radiants);
		double sin = Math.sin(radiants);
		
		// Rotate coordinates
		Point dest = new Point((double) (relativePoint.x * cos + relativePoint.y * sin),
				(double) (-relativePoint.x * sin + relativePoint.y * cos));
        
		return new Point(center.x + dest.x, center.y + dest.y);
	}
	
	/*
	 * 	Computes an integral image of a gray scale original image
	 * 	@param src Image source
	 */
	public static Mat getIntegralImage(Mat src) {
		Mat dest = new Mat();
	    Imgproc.integral(src, dest, CvType.CV_32S);
	    return dest;
	}
	
	/*
	 * 	Computes the value of a point using a liner interpolation
	 * 	@param img Image source
	 * 	@param point point to compute
	 * 	@return Linear interpolation of the given point
	 */
	public static double interpolatePoint(Mat img, Point point) {

	    // Calculate points
	    int y2 = (int) point.y;
	    int y1 = (int) Math.ceil(point.y);
	    int x2 = (int) Math.ceil(point.x);
	    int x1 = (int) point.x;
	    
		// Get original point
		double x = point.x;
	    double y = point.y;
	    
	    // Compute
	    double q11 = img.get(y1, x1)[0];
	    double q21 = img.get(y1, x2)[0];
	    double q12 = img.get(y2, x1)[0];
	    double q22 = img.get(y2, x2)[0];
	    
	    double r1 = (x2 - x)/(x2 - x1) * q11 + (x - x1)/(x2 - x1) * q21;
	    double r2 = (x2 - x)/(x2 - x1) * q12 + (x - x1)/(x2 - x1) * q22;
	    double result = (y2 - y)/(y2 - y1) * r1 + (y - y1)/(y2 - y1) * r2;
	    return result;
	}
	
	/*
	 * 	@param img Original image
	 * 	@param factor Desired resize factor
	 * 	@return Result of resizing original image by the given factor
	 */
	public static Mat resizeImage(Mat img, double factor) {
		Size resultSize = new Size(img.size().width * factor, img.size().height * factor);
		Mat result = new Mat(resultSize, img.type());
		Imgproc.resize(img, result, resultSize);
		return result;
	}
	
	/*
	 * 	@param source Original image
	 * 	@param kernelSize Size of the gaussian kernel
	 * 	@param sigma Sigma value of the gaussian filter
	 * 	@return Applies a gaussian filter of the given size and sigma on the original image
	 */
	public static Mat applyGaussian(Mat source, Size kernelSize, double sigma) {
		Mat result = new Mat(source.size(), source.type());
		Imgproc.GaussianBlur(source, result, kernelSize, sigma);
		return result;
	}
	
	/*
	 * 	@param source Original image
	 * 	@param top Width of the top side of the frame
	 * 	@param bottom Width of the bottom side of the frame
	 * 	@param left Width of the left side of the frame
	 * 	@param rigth Width of the right side of the frame
	 * 	@param type Type of the frame
	 * 	@return Result of applying a frame of the given type and dimensions on the original image
	 */
	public static Mat addBorder(Mat source, int top, int bottom, int left, int right, int type) {
		Mat result = new Mat(source.size(), source.type());
		Imgproc.copyMakeBorder(source, result, top, bottom, left, right, type);
		return result;
	}
	
	/*
	 * 	@param source Image source
	 * 	@param quality
	 * 	Compresses a JPEG file given a quality (float between 0 and 1)
	 */
	public static Mat getJPEGCompressedImage(Mat source, float quality) 
			throws InvalidArgumentException, IOException {
	    
		if (quality < 0.0f || quality > 1.0f) {
			throw new InvalidArgumentException("Quality is out of bounds: " + quality + ". Must be between 0.0 and 1.0");
		}
		
		// Define temporary pathfile path
		String tempDir = System.getProperty("java.io.tmpdir");
		String fileName = tempDir + File.separatorChar + ReplicaUtils.getRandomID() + "-q" + quality + ".jpeg";
		
		// Save file
		ReplicaUtils.matrixToFile(source, fileName);
		
		// Read input image
		InputStream iis = new FileInputStream(fileName);
		RenderedImage image = ImageIO.read(iis);
		
		// Define compression param
		JPEGImageWriteParam jpegParams = new JPEGImageWriteParam(null);
		jpegParams.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
		jpegParams.setCompressionQuality(quality);
		
		// Get default writer
		Iterator<ImageWriter>writers = ImageIO.getImageWritersByFormatName("jpeg");
	    ImageWriter writer = (ImageWriter) writers.next();

		// Specifies where the jpg image has to be written - byte array buffer
	    ByteArrayOutputStream os = new ByteArrayOutputStream();
	    ImageOutputStream ios = ImageIO.createImageOutputStream(os);
	    writer.setOutput(ios);

		// Writes the file with given compression level 
		writer.write(null, new IIOImage(image, null, null), jpegParams);
		
		// Delete temporary file
		ReplicaUtils.deleteFile(fileName);
		
		return ReplicaUtils.bytesToMatrix((int) source.size().height, (int)source.size().width, os.toByteArray());
	}
	
	/*
	 * 	Horizontal Cropping
	 * 
	 *  @param source Original image
	 * 	@param perc Percentage of image that will be cropped from the left side of the image
	 * 	@return Horizontally cropped image
	 */
	public static Mat cropImageH(Mat source, double perc) {
		double preserve = 1.0 - perc;
		double croppedWidth =  source.size().width * preserve;
		Rect rectCrop = new Rect(0, 0, (int) croppedWidth, (int) source.size().height);
		Mat result = source.clone();
		return new Mat(result, rectCrop);
	}
	
	/*
	 * 	Vertical cropping
	 * 
	 * 	@param perc Percentage of image that will be cropped from the top of the image
	 * 	Horizontally crops an image
	 */
	public static Mat cropImageV(Mat source, double perc) {
		double preserve = 1.0 - perc;
		double croppedHeight =  source.size().height * preserve;
		Rect rectCrop = new Rect(0, 0, (int) source.size().width, (int) croppedHeight);
		Mat result = source.clone();
		return new Mat(result, rectCrop);
	}
	
	/*
	 * 	Image rotation
	 * 
	 *  @param source Original image
	 * 	@param angle Anti-clockwise angle of rotation (in degrees)
	 * 	@return Rotated image
	 */
	public static Mat rotate(Mat source, double angle) {
		Mat result = new Mat();
	    Point center = new Point(source.cols()/2., source.rows()/2.);   
	    Mat rotationMat = Imgproc.getRotationMatrix2D(center, angle, 1.0);
	    Imgproc.warpAffine(source, result, rotationMat, new Size(source.cols(), source.rows()));
	    return result;
	}
	
	/*
	 *  Horizontal flipping
	 *  
	 * 	@param source Original image
	 * 	@return Image flipped horizontally
	 * 
	 */
	public static Mat horizontalFlipping(Mat source) {
		Mat result = new Mat();
	    Core.flip(source, result, 1);
	    return result;
	}
	
	/*
	 * 	Image filtering: Smoothes an image using a squared kernel with the specified size
	 * 
	 * 	@param source Image to smooth
	 * 	@param size Size of the side of the kernel
	 * 	@return Resulting smoothed image
	 * 
	 */
	public static Mat smoothImage(Mat source, int size) {
		Mat result = new Mat();
		Imgproc.blur(source, result, new Size(size, size));
		return result;
	}
	
	/*
	 * 	Gaussian Noise Addition: modifies an image adding additive gaussian noise to it
	 * 	using the specified parameters
	 * 
	 * 	@param source Original image
	 * 	@param mean Mean of the gaussian distribution. Values must fall between 0 and 1
	 * 	@param stdev Standard deviation of the gaussian distribution. Values must fall between 0 and 1
	 * 	@return Result of adding additive gaussian noise to the original image
	 */
	public static Mat addAdditiveGaussianNoise(Mat source, double mean, double stdev) throws InvalidArgumentException {
		
		if (mean < 0 || stdev < 0 || stdev > 1.0 || mean > 1.0) {
			throw new InvalidArgumentException("Mean and variance should fall in the interval [0.0-1.0]");
		}
		
		Mat result = new Mat(source.size(), source.type());
		Mat sourceNorm = new Mat();
		// Generate a random matrix with Gaussian values
		Core.randn(result, mean, stdev);
		// Normalize the original image into 0.0 - 1.0
		Core.normalize(source, sourceNorm, 0.0, 1.0, Core.NORM_MINMAX);
		// Add the matrix to the original one
		Core.add(sourceNorm, result, result);
		// Normalize so values are between 0 and 255
		Core.normalize(result, result, 0, 255, Core.NORM_MINMAX);
		return result;
	}
	
	/*
	 * 	Gamma correction: applies Gamma Correction technique to the original image
	 * 	
	 * 	@param source Original image
	 * 	@param gamma Gamma factor
	 * 	@return Result of applying Gamma Correction to the original image
	 */
	public static Mat applyGammaCorrection(Mat source, double gamma) {
		
		Mat result = new Mat(source.size(), source.type());
		
		for (int i = 0; i < source.rows(); ++i) {
			for (int j = 0; j < source.cols(); ++j) {
				double value[] = source.get(i,  j);
				value[0] =computeGammaValue(value[0], gamma);
				value[1] = computeGammaValue(value[1], gamma);
				value[2] = computeGammaValue(value[2], gamma);
				result.put(i, j, value);
			}
		}
		
		return result;
	}
	
	protected static double computeGammaValue(double value, double gamma) {
		return 255.0 * Math.pow(value/255.0, gamma);
	}
	
	/*
	 * 	Saturation channel modification from a HSV image
	 * 	
	 * 	@param img Original HSV image to modify
	 * 	@param perc Modification ratio
	 * 	@return Result of modifying the saturation channel of the original image using the given ratio
	 */
	public static Mat modifySChannel(Mat img, double perc) throws InvalidArgumentException {
		return modifyChannel(img, perc, 1);
	}
	
	/*
	 * 	Value channel modification from a HSV image
	 * 	
	 * 	@param img Original HSV image to modify
	 * 	@param perc Modification ratio
	 * 	@return Result of modifying the saturation channel of the original image using the given ratio
	 */
	public static Mat modifyVChannel(Mat img, double perc) throws InvalidArgumentException {
		return modifyChannel(img, perc, 2);
	}
	
	/*
	 * 	Modifies the given channel from the HSV image
	 * 	
	 * 	@param img Original HSV image to modify
	 * 	@param perc Modification ratio
	 * 	@param index Index of the channel to modify
	 * 	@return Result of modifying the specified channel of the original image using the given ratio
	 */
	protected static Mat modifyChannel(Mat img, double perc, int channel) throws InvalidArgumentException {
		
		if (img.channels() != 3) {
			throw new InvalidArgumentException("Image should have 3 channels: H,S and V");
		}
		
		if (perc < -1.0 || perc > 1.0) {
			throw new InvalidArgumentException("Percentage must be between -1 and 1");
		}
		
		if (channel < 0 || channel > 2) {
			throw new InvalidArgumentException("Channel index must be between 0 and 2");
		}
		
		Mat result = img.clone();

		for (int i = 0; i < img.rows(); ++i) {
			for (int j = 0 ; j < img.cols(); ++j) {
				double value[] = result.get(i,  j);
				value[channel] = value[channel] + value[channel] * perc;
				result.put(i, j, value);
			}
		}
		
		return result;
	}

	/*
	 * 	Image occlusion: Partially occludes an image using black circles randomly positioned 
	 * 	on the images
	 * 	
	 * 	@param img Source image
	 * 	@param circles Number of circles that are used for occlusion
	 * 	@param radius Size of the radius of each circle. Must be smaller than the image width
	 * 	@return Result of partially occluding the given image
	 */
	public static Mat occludeImage(Mat img, int circles, int radius) {
		Mat result = img.clone();
		Size size = img.size();
		int hOffset = (int) size.width - radius;
		int vOffset = (int) size.height - radius;
		for (int i = 1; i <= circles; ++i) {
			Point randomCenter = new Point(ReplicaUtils.getUniformRandomInc(hOffset, size.width - hOffset), 
					ReplicaUtils.getUniformRandomInc(vOffset, size.height - vOffset));
			Core.circle(result, randomCenter, radius, new Scalar(0, 0 ,0), -1);
		}
		return result;
	}
	
	/*
	 * 	Image occlusion: Partially occludes an image using black circles randomly positioned 
	 * 	on the images
	 * 	
	 * 	@param img Source image
	 * 	@param circles Number of circles that are used for occlusion
	 * 	@param perc Percentage (0.0 - 1.0) of the largest side of the image from which we compute the circle radius
	 * 	@return Result of partially occluding the given image
	 */
	public static Mat occludeImage(Mat img, int circles, double perc) {
		Mat result = img.clone();
		Size size = img.size();
		double largSide = (int) Math.max(size.height, size.width);
		int radius = (int) (largSide * perc);
		int hOffset = (int) size.width - radius;
		int vOffset = (int) size.height - radius;
		for (int i = 1; i <= circles; ++i) {
			Point randomCenter = new Point(ReplicaUtils.getUniformRandomInc(hOffset, size.width - hOffset), 
					ReplicaUtils.getUniformRandomInc(vOffset, size.height - vOffset));
			Core.circle(result, randomCenter, radius, new Scalar(0, 0 ,0), -1);
		}
		return result;
	}
	
	/*
	 * 	Text addition: Horizontally sets fixed size white text into a random location of the image
	 * 	
	 * 	@param img Source image
	 * 	@param length Length of the text to insert
	 * 	@return Result of inserting a random text of the specified length into the original image
	 */
	public static Mat addText(Mat img, int length) {
		Mat result = img.clone();
		Size size = img.size();
		Point randomPoint = new Point(ReplicaUtils.getRandomInteger((int) size.height), 
				ReplicaUtils.getRandomInteger((int)size.width / 2 ));
		Core.putText(result, ReplicaUtils.getRandomString(length), randomPoint, Core.FONT_HERSHEY_TRIPLEX, 
				1.0, new Scalar(255, 255, 255));
		return result;
	}
	
	/*
	 * 	Computes the intensity image histogram of the given matrix and accumulates it
	 * 	@param matrix Matrix to compute histogram from
	 * 	@param hist Matrix where to accumulate the histogram
	 */
	public static void accumulateHistogram(Mat image, Mat hist) {
		ArrayList<Mat> images = new ArrayList<Mat>();
		images.add(image);
		// Specify ranges
		float rangesArray[] = { 0.0f, 256.0f };
		MatOfFloat ranges = new MatOfFloat(rangesArray);
		// Size
		int sizeArray[]={ 256 };
        MatOfInt histSize=new MatOfInt(sizeArray);
		Imgproc.calcHist(images, new MatOfInt(0), new Mat(), hist, histSize, ranges, true);
	}
	
	/*	
	 * 	Extracts the ith feature vector as double array 
	 * 	@param mat Original feature
	 * 	@return feature vector
	 */
	public static double[] matToArray(Mat m, int i) {
		double[] currentRow = new double[(int) m.size().width];
		for (int j = 0; j < m.size().width; ++j) {
			currentRow[j] = m.get(i, j)[0];
		}
		return currentRow;
	}
}
