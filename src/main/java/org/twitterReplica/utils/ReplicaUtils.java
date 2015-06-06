package org.twitterReplica.utils;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.math.BigInteger;
import java.net.URL;
import java.security.SecureRandom;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.util.collection.BitSet;
import org.opencv.core.CvType;
import org.opencv.core.Mat;
import org.opencv.core.Size;
import org.opencv.highgui.Highgui;
import org.opencv.utils.Converters;

import com.sun.management.OperatingSystemMXBean;

@SuppressWarnings("restriction")
public class ReplicaUtils {

	private static final SecureRandom random = new SecureRandom();
	public static final double log2 = Math.log(2);
	private static OperatingSystemMXBean measure =  (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
	
	public final static OperatingSystemMXBean getMeasure() {
		return measure;
	}
	
	/*
	 * 	Measure the time in nanoseconds when called
	 */
	public final static long getCPUTimeNow() {
		return measure.getProcessCpuTime();
	}
	
	/*
	 * 	@param Length of the string to generate
	 * 	@return Generates a string of the given length made of numeric characters
	 */
	public static String getRandomString(int length) {
		return new BigInteger(130, random).toString(length);
	}
	
	/*
	 * 	@param max Upper bound of the number to generate
	 * 	@return Returns a random number integer in the uniformly distributed between [0, max)
	 */
	public static int getRandomInteger(int max) {
		  return Math.abs(random.nextInt(max));
	}
	
	/*
	 * 	Returns a random uniformly distributed double in the distribution [0, 1]
	 */
	public static double getRandomDouble() {
		return random.nextDouble();
	}
	
	/*
	 * 	@return Timestamp right now (hours:minutes:seconds)
	 */
	public static String getTimeNow() {
		Calendar cal = Calendar.getInstance();
    	cal.getTime();
    	SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
    	return sdf.format(cal.getTime());
	}
	
	/*
	 * 	Prints the input matrix into the standard output
	 * 
	 * 	@param m Matrix to print
	 */
	public static void printMatrix(Mat m) {
		String dump = m.dump();
		System.out.println(dump);
	}
	
	/*
	 * 	Loads a color image from the given address and with the given height and weight
	 * 	Source: http://answers.opencv.org/question/10236/opencv-java-api-highguiimread-and-paths-pointing/
	 */
	public static Mat getImageContentFromURL(URL url) throws IOException {
		
		// Initialization
		byte pixels[] = new byte[3];
		int rgb;
		
		// Read image
		BufferedImage buf = ImageIO.read(url);
		int height = buf.getHeight();
        int width = buf.getWidth();

        
		Mat result = new Mat(height, width, CvType.CV_8UC3);
		// Go through read bytes and put them into matrix
         for (int y=0; y<height; y++) {
             for (int x=0; x<width; x++) {
                 rgb = buf.getRGB(x, y);
                 pixels[0] = (byte)(rgb & 0xFF);
                 pixels[1] = (byte)((rgb >> 8) & 0xFF);
                 pixels[2] = (byte)((rgb >> 16) & 0xFF);
                 result.put(y, x, pixels);
             }
         }     
         
         return result;
         // We could also call the line below but 
         //return bufferedImageToMat(buf);
	}
	
	/*
	 * 	Converts buffered image into matrix
	 * 	@param buf Input buffered image
	 */
	protected static Mat bufferedImageToMat(BufferedImage buf) throws IOException {
		
		// Get bytes
    	byte[] bytes = bufferedImageToBytes(buf);
		
    	// Map to Bytes
		Byte[] bigByteArray = new Byte[bytes.length];
        for (int i=0; i < bytes.length; i++)                        
            bigByteArray[i] = new Byte(bytes[i]); 
        
        // Convert bytes to matrix
        List<Byte> matlist = Arrays.asList(bigByteArray);
    	Mat img = new Mat();
        img = Converters.vector_char_to_Mat(matlist);
        img = Highgui.imdecode(img, Highgui.CV_LOAD_IMAGE_COLOR); 
        return img;
	}
	
	/*
	 *	Converts a buffered image into bytes
	 */
	protected static byte[] bufferedImageToBytes(BufferedImage buf) throws IOException {
		// Get bytes
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
    	ImageIO.write(buf, "jpg", baos);
    	baos.flush();
    	byte[] bytes = baos.toByteArray();
    	baos.close();
    	return bytes;
	}
	
	/*
	 * 	@param height Height of the matrix in pixels
	 * 	@param width Width of the matrix in pixels
	 * 	@param bytes Content of the matrix
	 * 	@return Matrix representation of the input content
	 */
	public static final Mat bytesToMatrix(int height, int width, byte[] bytes) {
		Mat raw = new Mat(height, width, CvType.makeType(0, 1));
        raw.put(0, 0, bytes);
		Mat result = Highgui.imdecode(raw, Highgui.CV_LOAD_IMAGE_COLOR);
		return result;
	}
	
	/*
	 * 	This function writes a matrix into a file. WARNING: only works on local file system
	 * 	@param mat Matrix to dump
	 * 	@param path Path where to store the matrix content
	 * 	@return Stores the content of the matrix into the given path
	 */
	public static void matrixToFile(Mat mat, String path) throws IOException {
		Highgui.imwrite(path, mat);
	}
	
	
	/*
	 * 	Returns whether two matrices has same size
	 * 	@param first First matrix
	 * 	@param second Second matrix
	 * 	@return Whether the two matrices have the same height and width
	 */
	public static boolean haveSameSize(Mat m1, Mat m2) {
		return m1.size().height == m2.size().height && m1.size().width == m2.size().width;
	}
	
	/*
	 * 	@param path File to delete
	 * 	Deletes the file in the given path
	 */
	public static boolean deleteFile(String path) {
		File file = new File(path);
		return file.delete();
	}
	
	/*
	 * 	@return Returns a random numeric identifier
	 */
	public static String getRandomID() {
	    return new Integer(random.nextInt()).toString();
	}
	
	/*
	 * 	@param path Path where to create an empty folder
	 * 	Creates an empty string
	 */
	public static void createFolder(String path) {
		File dir = new File(path);
		dir.mkdir();
	}
	
	/*
	 * 	Checks whether the given path is a folder
	 * 	@param path Path of the folder whose existance we want to check
	 */
	public static boolean folderExists(String path) {
		return new File(path).isDirectory();
	}
	
	/*
	 * 	Extension
	 * 
	 * 	@param ext Extension of the image
	 * 	@param list List of accepted extensions
	 * 	@return whether the image format is contained in the acceptance list
	 */
	public static boolean isExtAccepted(String ext, String[] list) {
		for (String s : list) {
			if (ext.equalsIgnoreCase(s)) {
				return true;
			}
		}
		return false;
	}
	
	/*
	 * 	Extension
	 * 
	 * 	@param src Size of the image
	 * 	@param min Minimum size accepted
	 * 	 * 	@param max Maximum size accepted
	 * 	@return whether the image meets the requirements of size taking into account the minimum accepted size
	 */
	public static boolean isSizeAccepted(Size src, Size min, Size max) {
		return (src.width >= min.width && src.height >= min.height) &&
				(src.width <= max.width && src.height <= max.height);
	}
	
	/*
	 * 	Converts time in nanoseconds to milliseconds
	 */
	public static double nanoToMillis(double nanos) {
		return nanos / (double) 1e6;
	}
	
	/*
	 * 	Get the unary representation of the discretized input real number
	 * 	@param max Maximum number to be represented
	 *  @param num Number to convert to convert to unary. Cannot be bigger than max
	 * 	@return Unary representation of the given number
	 */
	public static char[] doubleToUnary(int max, double num) {
		char[] chars = new char[max];
		int discrNum = (int) num;
		// Fill with ones
		for (int i = 0; i <= num; ++i) {
			chars[i] = '1';
		}
		// Fill with zeros
		for (int i = discrNum + 1; i < max; ++i) {
			chars[i] = '0';
		}
		return chars;
	}
	
	/*
	 * 	Get the bit-representation of discretized representation of the number
	 * 	@param num Number to convert to binary
	 * 	@return Binary representation of the given number
	 */
	public static String doubleDiscretizedToBits(double num) {
		return Long.toBinaryString((int) num);
	}
	
	/*
	 * 	Get the bit-representation of the number
	 * 	@param num Number to convert to binary
	 * 	@return Binary representation of the given number
	 */
	public static String doubleToBits(double num) {
		return Long.toBinaryString(Double.doubleToLongBits(num));
	}
	
	/*
	 * 	Returns a random uniformly distributed double in the distribution [min, max]
	 */
	public static double getUniformRandomInc(double min, double max) {
		return min	+ (max - min) * random.nextDouble();
	}
	
	/*
	 * 	Check whether two matrices are equal pixel per pixel
	 * 	@param first First matrix
	 * 	@param second Second matrix
	 	@return whether the matrices are the same
	 */
	public static boolean areMatricesEqual(Mat first, Mat second) {
		
		if (!ReplicaUtils.haveSameSize(first, second)) {
			return false;
		}
		
		for (int i = 0; i < first.size().height; ++i) {
			for (int j = 0; j < first.size().width; ++j) {
				if (first.get(i, j)[0] != second.get(i, j)[0]) {
					return false;
				}
			}
		}
		
		return true;
	}
	
	/*
	 * 	From: http://stackoverflow.com/questions/2473597/bitset-to-and-from-integer-long
	 * 	Serializes a bit set into a integer. Warning: it is up to the user to ensure that
	 * 	the input bit set fits into an integer.
	 * 	@param bits Bit set
	 * 	@return long formatted bit set
	 */
	public static int toInt(BitSet bits) {
		int value = 0;
		for (int i = 0; i < bits.capacity(); ++i) {
			value += bits.get(i) ? (1 << i) : 0;
		}
		return value;
	}

	/*
	 * 	From: http://stackoverflow.com/questions/2473597/bitset-to-and-from-integer-long
	 *  Deserializes a bitset from a long
	 * 	@param value Long formatted bit set
	 */
	public static BitSet fromInt(int value) {
		BitSet bits = new BitSet(32);
		int index = 0;
		while (value != 0L) {
			if (value % 2L != 0) {
				bits.set(index);
			}
			++index;
			value = value >>> 1;
		}
		return bits;
	}
	
	/*
	 * 	Prints a bit set representation
	 */
	public static void printBitSet(BitSet bits) {
		for (int i = 1; i <= 32; ++i) {
			char print = bits.get(32 - i) ? '1' : '0';
			System.out.print(print);
		}
	}
	
	public static void printBitSetArray(BitSet[] array) {
		int length = array.length;
		for (int i = length - 1; i >= 0; --i) {
			System.out.print("|");
			printBitSet(array[i]);
		}
		System.out.println();
	}
	
	/*
	 * 	Reads serialized list of double into an array
	 * 	@param list List of comma-separated double
	 * 	@return Double array
	 */
	public static double[] getArrayFromString(String list) {
		String[] splitList = list.split(",");
		double[] result = new double[splitList.length];
		for (int i = 0; i < splitList.length; ++i) {
			result[i] = Double.valueOf(splitList[i]);
		}
		return result;
	}
	
	/*
	 * 	Serializes a list of double into a comma-separated string
	 * 	@param list List of double
	 * 	@return Resulting string
	 */
	public static String listToString(double[] list) {
		StringBuilder result = new StringBuilder();
		for (int i = 0; i < list.length; ++i) {
			if (i != 0) {
				result.append(",");
			}
			result.append(String.valueOf(list[i]));
		}
		return result.toString();
	}
	
	/*
	 * 	@param path Input path
	 * 	@return whether the input path is an URL
	 */
	public static boolean isURL(String path) {
		return path.startsWith("http:") || path.startsWith("https:");
	}
	
	/*
	 * 	@param path Input path
	 * 	@return whether the input path exists as a file in the local file system
	 */
	public static boolean fileExists(String path) {
		File file = new File(path);
		return file.exists() || file.isFile();
	}
	
	/*
	 * 	@param path Input path
	 * 	@return whether the input path belongs to a Hadoop Distributed File System
	 */
	public static boolean isHDFS(String path) {
		return path.startsWith("hdfs://");
	}
	
	/*
	 * 	Read HDFS from file
	 */
	public static Mat readFromHDFS(String filePath) throws IOException {
		// Read from HDFs stream
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = fs.open(new Path(filePath));
		BufferedImage im = ImageIO.read(in);
		in.close();
		// Map into matrix
		return bufferedImageToMat(im);
	}
	
}
