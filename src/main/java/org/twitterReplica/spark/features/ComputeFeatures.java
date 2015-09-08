package org.twitterReplica.spark.features;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.twitterReplica.core.DescriptorParams;
import org.twitterReplica.improcessing.ImageTools;
import org.twitterReplica.model.Image;
import org.twitterReplica.model.ImageFeature;
import org.twitterReplica.model.ImageInfo;
import org.twitterReplica.model.providers.ProviderType;

import scala.Tuple2;

public class ComputeFeatures implements PairFlatMapFunction<ImageInfo, ImageInfo, ImageFeature> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private DescriptorParams parameters;
	private ProviderType pType;
	
	public ComputeFeatures(DescriptorParams parameters, ProviderType provider) {
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
	public Iterable<Tuple2<ImageInfo, ImageFeature>> call(ImageInfo line) throws Exception {
			
		// Patch to make it work on BSC
		System.loadLibrary( Core.NATIVE_LIBRARY_NAME );
		
		List<Tuple2<ImageInfo, ImageFeature>> features = new ArrayList<Tuple2<ImageInfo, ImageFeature>>();
		
		// Read image
		Mat img = Image.readImageFromPath(line.getPath());
		
		// Compute descriptor
		Image imRes = new Image(img, line.getPath(), line.getId(), line.getResourceId(), this.pType);
		imRes.computeDescriptor(this.parameters);
		Mat desc = imRes.getDescriptor();
		
		// Map feature per each line in descriptor
		for(int i = 0; i < desc.size().height; ++i) {
			double[] feature = ImageTools.matToArray(desc, i);
			features.add(new Tuple2<ImageInfo, ImageFeature>(line, 
					new ImageFeature(line.getId(), feature, this.parameters.getDescriptorType())));
		}
		
		return features;
	}

}
