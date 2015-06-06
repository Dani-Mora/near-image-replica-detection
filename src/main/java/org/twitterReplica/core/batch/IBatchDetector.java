package org.twitterReplica.core.batch;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.twitterReplica.exceptions.IndexingException;
import org.twitterReplica.exceptions.QueryException;
import org.twitterReplica.model.ImageInfo;
import org.twitterReplica.model.ImageMatch;

public interface IBatchDetector {

	/*
	 * 	Indexes data from the UPCReplica dataset
	 * 	@param spark Spark Java Context
	 * 	@param selected File containing rows of images to index
	 * 	@param datasetPath UPCReplica dataset root folder
	 * 	@param reset Whether to flush all data stored before indexing
	 */
	public void indexFromDataset(JavaSparkContext spark, String selected, String datasetPath, boolean reset) throws IndexingException;
	
	/*
	 * 	Indexes images from the input folder
	 * 	List of extensions supported: .bmp, .dib, .jpeg, .jpg, .jpe, .jp2, .png, .pbm, 
	 * 		.pgm, .ppm, .sr, .ras, .tiff, .tif
	 * 	@param spark Spark Java Context
	 * 	@param folder Input folder where to extract images from
	 * 	@param reset Whether to flush all data stored before indexing
	 */
	public void indexFromFolder(JavaSparkContext spark, String folder, boolean reset) throws IndexingException;
	
	/*
	 * 	Queries and image and retrieves the matches and their weights
	 * 	List of extensions supported: .bmp, .dib, .jpeg, .jpg, .jpe, .jp2, .png, .pbm, 
	 * 		.pgm, .ppm, .sr, .ras, .tiff, .tif
	 * 	@param spark Spark Context
	 * 	@param imRes Image to query
	 * 	@param rank Feature filtering threhsold. Matches below this number of matches will be discarded
	 * 	@return RDD containing the pairs of identifiers and weights of the image matched
	 */
	public JavaPairRDD<ImageMatch, Long> queryImage(JavaSparkContext spark, ImageInfo imRes, int rank) throws QueryException;
	
	/*
	 * 	Queries images selected from the UPCReplica dataset
	 *	@param spark Spark Java Context
	 * 	@param selected File containing rows of images to index
	 * 	@param datasetPath UPCReplica dataset root folder
	 * 	@param rank Feature filtering threhsold. Matches below this number of matches will be discarded
	 * 	@return RDD containing the pairs of identifiers and weights of the image matched
	 */
	public JavaPairRDD<ImageMatch, Long> queryFromDataset(JavaSparkContext spark, String selected, String datasetPath, 
			int rank) throws QueryException;
	
	/*
	 * 	Queries images selected from the UPcReplica dataset
	 *	@param spark Spark Java Context
	 * 	@param selected File containing rows of images to index
	 * 	@param datasetPath UPCReplica dataset root folder
	 * 	@param rank Feature filtering threhsold. Matches below this number of matches will be discarded
	 * 	@return RDD containing the pairs of identifiers and weights of the image matched
	 */
	public JavaPairRDD<ImageMatch, Long> queryFromFolder(JavaSparkContext spark, String folder, int rank) throws QueryException;

}
