package org.twitterReplica.spark.features;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.opencv.core.Mat;
import org.opencv.highgui.Highgui;
import org.opencv.utils.Converters;
import org.twitterReplica.core.DescriptorParams;
import org.twitterReplica.improcessing.ImageTools;
import org.twitterReplica.model.Image;
import org.twitterReplica.model.ImageFeature;
import org.twitterReplica.model.ImageInfo;
import org.twitterReplica.model.providers.ProviderType;

import scala.Tuple2;

public class SeqFileComputeFeatures implements PairFlatMapFunction<Tuple2<Text, BytesWritable>, ImageInfo, ImageFeature> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private DescriptorParams parameters;
	private ProviderType pType;
	
	public SeqFileComputeFeatures(DescriptorParams parameters, ProviderType provider) {
		super();
		this.parameters = parameters;
		this.pType = provider;
	}

	/*
	 * 	Given an set of image inputs computes its image features
	 * 	@param images Input images
	 * 	@param descParams Descriptor parameters
	 * 	@return Pair containing image identifier and list of image features computed
	 */
	public Iterable<Tuple2<ImageInfo, ImageFeature>> call(Tuple2<Text, BytesWritable> line) throws Exception {
			
		List<Tuple2<ImageInfo, ImageFeature>> features = new ArrayList<Tuple2<ImageInfo, ImageFeature>>();
		
		// Compute descriptor
		String path = line._1.toString();
		long id = path.hashCode();
		Mat img = byteswritableToMat(line._2);
		
		Image imRes = new Image(img, path, id, path, this.pType);
		imRes.computeDescriptor(this.parameters);
		Mat desc = imRes.getDescriptor();
		
		// Map feature per each line in descriptor
		for(int i = 0; i < desc.size().height; ++i) {
			double[] feature = ImageTools.matToArray(desc, i);
			features.add(new Tuple2<ImageInfo, ImageFeature>(new ImageInfo(path, id), 
					new ImageFeature(id, feature, this.parameters.getDescriptorType())));
		}
		
		return features;
	}
	
	/*	
	 * 	Converts input bytes into a matrix
	 * 	From: http://personals.ac.upc.edu/rtous/howto_spark_opencv.xhtml
	 */
	public static Mat byteswritableToMat(BytesWritable inputBW) {
        // Compute input bytes
		byte[] imageFileBytes = inputBW.getBytes();
        Byte[] bigByteArray = new Byte[imageFileBytes.length];
        for (int i=0; i<imageFileBytes.length; i++) {
            bigByteArray[i] = new Byte(imageFileBytes[i]);
        }
        // To list
        List<Byte> matlist = Arrays.asList(bigByteArray);       

        // Convert into image matrix
        Mat img = new Mat();
        img = Converters.vector_char_to_Mat(matlist);
        img = Highgui.imdecode(img, Highgui.CV_LOAD_IMAGE_COLOR);         
        return img;
    }
	

}
