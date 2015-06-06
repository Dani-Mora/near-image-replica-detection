package org.twitterReplica.core.batch;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.opencv.core.Mat;
import org.twitterReplica.core.DescriptorParams;
import org.twitterReplica.core.ReplicaConnection;
import org.twitterReplica.core.ReplicaSystem;
import org.twitterReplica.exceptions.DataException;
import org.twitterReplica.exceptions.IndexingException;
import org.twitterReplica.exceptions.QueryException;
import org.twitterReplica.improcessing.ImageTools;
import org.twitterReplica.model.FilteringType;
import org.twitterReplica.model.Image;
import org.twitterReplica.model.ImageFeature;
import org.twitterReplica.model.ImageInfo;
import org.twitterReplica.model.ImageMatch;
import org.twitterReplica.model.PersistenceMode;
import org.twitterReplica.model.providers.ProviderType;
import org.twitterReplica.spark.features.ComputeFeatures;
import org.twitterReplica.spark.features.EntropyFiltering;
import org.twitterReplica.spark.features.LogScaleFunction;
import org.twitterReplica.spark.features.SketchProcedure;
import org.twitterReplica.spark.features.VarianceFiltering;
import org.twitterReplica.spark.input.DescriptorDiskReader;
import org.twitterReplica.spark.input.FolderReader;
import org.twitterReplica.spark.query.AccumulateWeightQuery;
import org.twitterReplica.spark.query.HammingFiltering;
import org.twitterReplica.spark.query.MatchExtractorWeight;
import org.twitterReplica.spark.query.WeightFiltering;
import org.twitterReplica.utils.ReplicaUtils;

import scala.Tuple2;

import com.google.common.io.Files;

public class BatchReplicaDetector extends ReplicaSystem implements IBatchDetector {

	// List of allowed image extensions
	protected static final String[] IMG_EXTENSIONS_ARRAY = new String[] { "bmp", "dib", "jpeg",
			"jpg", "jpe", "jp2", "png", "pbm", "pgm", "ppm", "sr", "ras", "tiff", "tif" };
	public static final List<String> IMG_EXTENSIONS = Arrays.asList(IMG_EXTENSIONS_ARRAY);
	
	public BatchReplicaDetector(PersistenceMode mode, ReplicaConnection conn) {
		super(mode, conn);
	}

	@Override
	public ImageInfo getImage(long id) throws DataException {
		return system.getImage(conn, id);
	}
	
	@Override
	public void indexFromDataset(JavaSparkContext spark, String selected,
			String datasetPath, boolean reset) throws IndexingException {
		
		// if reset requested, restart all data
		if (reset) {
			try {
				system.restartData(conn, indParams);
			} catch (DataException e) {
				throw new IndexingException("Could not restart data: " + e.getMessage());
			}
		}
		
		// Read images list from the descriptor and compute features
		JavaRDD<String> lines = spark.textFile(selected);
		JavaRDD<ImageInfo> images = lines.map(new DescriptorDiskReader(datasetPath));
		indexImages(images);
	}

	@Override
	public void indexFromFolder(JavaSparkContext spark, String folder, boolean reset) 
			throws IndexingException {

		if (!ReplicaUtils.folderExists(folder)) {
			throw new IndexingException("Input path '" + folder + "'is not a valid folder");
		}

		// if reset requested, restart all data
		if (reset) {
			try {
				system.restartData(conn, indParams);
			} catch (DataException e) {
				throw new IndexingException("Could not restart data: " + e.getMessage());
			}
		}
		
		// Get valid images from folder
		File folderF = new File(folder);
		String[] files = folderF.list(new FilenameFilter(){
			@Override
			public boolean accept(File dir, String name) {
				return IMG_EXTENSIONS.contains(Files.getFileExtension(name));
			}
		});

		// Parallelize, read image data and index
		JavaRDD<String> names = spark.parallelize(Arrays.asList(files));
		JavaRDD<ImageInfo> imgs = names.map(new FolderReader(folder));
		indexImages(imgs);
	}
	
	@Override
	public JavaPairRDD<ImageMatch, Long> queryFromDataset(JavaSparkContext spark, String selected, String datasetPath,
			int rank) throws QueryException {
		// Read images list from the descriptor and compute features
		JavaRDD<String> lines = spark.textFile(selected);
		JavaRDD<ImageInfo> images = lines.map(new DescriptorDiskReader(datasetPath));
		return queryImages(images, rank);
		
	}

	@Override
	public JavaPairRDD<ImageMatch, Long> queryFromFolder(JavaSparkContext spark, String folder, int rank) throws QueryException {
		
		if (!ReplicaUtils.folderExists(folder)) {
			throw new QueryException("Input path '" + folder + "'is not a valid folder");
		}
		
		// Get valid images from folder
		File folderF = new File(folder);
		String[] files = folderF.list(new FilenameFilter(){
			@Override
			public boolean accept(File dir, String name) {
				return IMG_EXTENSIONS.contains(Files.getFileExtension(name));
			}
		});

		// Parallelize, read image data and index
		JavaRDD<String> names = spark.parallelize(Arrays.asList(files));
		JavaRDD<ImageInfo> imgs = names.map(new FolderReader(folder));
		return queryImages(imgs, rank);
	}
	
	@Override
	public JavaPairRDD<ImageMatch, Long> queryImage(JavaSparkContext spark, ImageInfo imRes, int rank)
		throws QueryException {
		
		String path = imRes.getPath();
		
		// Check extension
		String imgExtension = Files.getFileExtension(path);
		if (!IMG_EXTENSIONS.contains(imgExtension)) {
			throw new QueryException("Extension '" + imgExtension + "' not accepted.");
		}
		
		// Read image
		Mat img = null;
		try {
			img = Image.readImageFromPath(path);
		} catch (IOException e) {
			throw new QueryException(e.getMessage());
		} 

		// Compute descriptor, parallelize vectors and query features
		Image imr = new Image(img, imRes.getPath(), imRes.getId(), String.valueOf(imRes.getId()), ProviderType.DISK);
		imr.computeDescriptor(descParams);
		JavaPairRDD<ImageInfo, ImageFeature> features = rddFromMatrix(spark, imRes, 
				descParams, imr.getDescriptor());
		return queryImage(features, rank);
	}
	
	/*
	 * 	Indexes a list of images
	 * 	@param images List of image metadata
	 */
	protected void indexImages(JavaRDD<ImageInfo> images) {	
		// Get image sketches
		JavaPairRDD<ImageInfo, ImageFeature> sketches = computeSketches(images);	
		system.indexData(conn, indParams, sketches);
	}
	
	/*
	 * 	Queries the set of input images
	 * 	@param images List of image metadata
	 */
	protected JavaPairRDD<ImageMatch, Long> queryImages(JavaRDD<ImageInfo> imgs, int rank) {
		// Get image sketches
		JavaPairRDD<ImageInfo, ImageFeature> sketches = computeSketches(imgs);	
		
		// Query specific and filter by hamming distance
		JavaPairRDD<ImageFeature, ImageFeature> candidates = system.queryFeatures(conn,indParams, sketches);
		JavaPairRDD<ImageFeature, ImageFeature> filteredHamming = 
				candidates.filter(new HammingFiltering(indParams.getHammingDistance()));
		
		// Group by image and assign weights
		JavaPairRDD<ImageMatch, Long> matchedIds = filteredHamming.mapToPair(new MatchExtractorWeight());
		JavaPairRDD<ImageMatch, Long> weighted = matchedIds.reduceByKey(new AccumulateWeightQuery());
		
		// Filter by weight if requested
		if (rank > 0) {
			weighted = weighted.filter(new WeightFiltering(rank));
		}
		
		return weighted;
	}
	
	/*
	 * 	Queries the input sketches
	 * 	@param features Input features
	 * 	@param rank Weight filtering threshold
	 * 	@return Matches from the input sketched features
	 */
	protected JavaPairRDD<ImageMatch, Long> queryImage(JavaPairRDD<ImageInfo, ImageFeature> features, int rank) {
		JavaPairRDD<ImageInfo, ImageFeature> sketches = computeSketches(features);
		return querySketches(sketches, rank);
	}
	
	/*
	 * 	Queries the input sketches
	 * 	@param sketches Input feature sketches
	 * 	@param rank Weight filtering threshold
	 * 	@return Image matched from the input sketches and their weights
	 */
	protected JavaPairRDD<ImageMatch, Long> querySketches(JavaPairRDD<ImageInfo, ImageFeature> sketches, int rank) {
		// Query specific and filter by hamming distance
		JavaPairRDD<ImageFeature, ImageFeature> candidates = system.queryFeatures(conn,indParams, sketches);
		JavaPairRDD<ImageFeature, ImageFeature> filteredHamming = 
				candidates.filter(new HammingFiltering(indParams.getHammingDistance()));
		
		// Group by image and assign weights
		JavaPairRDD<ImageMatch, Long> matchedIds = filteredHamming.mapToPair(new MatchExtractorWeight());
		JavaPairRDD<ImageMatch, Long> weighted = matchedIds.reduceByKey(new AccumulateWeightQuery());
		
		// Filter by weight if requested
		if (rank > 0) {
			weighted = weighted.filter(new WeightFiltering(rank));
		}
		
		return weighted;
	}
	
	/*
	 * 	Compute sketches from the input images
	 * 	@param imgs Set of images to compute sketches from
	 */
	protected JavaPairRDD<ImageInfo, ImageFeature> computeSketches(JavaRDD<ImageInfo> imgs) {
		// Compute feature vectors
		JavaPairRDD<ImageInfo, ImageFeature> features = 
				imgs.flatMapToPair(new ComputeFeatures(descParams, ProviderType.DISK));
		return computeSketches(features);
	}
	
	/*
	 * 	Compute sketches from the input features
	 * 	@param imgs Set of features to compute sketches from
	 */
	protected JavaPairRDD<ImageInfo, ImageFeature> computeSketches(JavaPairRDD<ImageInfo, ImageFeature> features) {
		
		// Filter descriptors if needed
		JavaPairRDD<ImageInfo, ImageFeature> filtered = features;
		if (filtParams.getFilteringType().equals(FilteringType.ENTROPY)) {
			filtered = features.filter(new EntropyFiltering(filtParams.getThresh()));
		}
		else if (filtParams.getFilteringType().equals(FilteringType.VARIANCE)) {
			filtered = features.filter(new VarianceFiltering(filtParams.getThresh()));
		}
		
		// Logscale features if needed
		if (filtParams.isLogScaleEnabled()) {
			filtered = filtered.mapValues(new LogScaleFunction());
		}

		// Get image sketches
		return filtered.mapValues(new SketchProcedure(indParams.getSketchFunction(), indParams.getNumTables()));
	}
	
	/*
	 * 	Converts an image descriptor into a Spark RDD of features
	 * 	so each entry is a feature of the image
	 * 	@param spark Java Spark context
	 * 	@param img Image object
	 * 	@param params Descriptor computation parameters
	 * 	@param desc Descriptor matrix
	 * 	@return JavaRDD made of features extracted from the image
	 */
	protected static JavaPairRDD<ImageInfo, ImageFeature> rddFromMatrix(JavaSparkContext spark, ImageInfo img, 
			DescriptorParams params, Mat desc) {
		List<Tuple2<ImageInfo, ImageFeature>> features = new ArrayList<Tuple2<ImageInfo, ImageFeature>>();
		for (int i = 0; i < desc.size().height; ++i) {
			features.add(new Tuple2<ImageInfo, ImageFeature>(img, 
					new ImageFeature(img.getId(), ImageTools.matToArray(desc, i), params.getDescriptorType())));
		}
		return spark.parallelizePairs(features);
	}

}
