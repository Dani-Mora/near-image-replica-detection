package org.twitterReplica.spark.features;

import org.apache.spark.api.java.function.Function;
import org.twitterReplica.model.ImageFeature;
import org.twitterReplica.model.SketchFunction;

public class SketchProcedure implements Function<ImageFeature, ImageFeature> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -562480784690088488L;

	private SketchFunction sketch;
	private int numTables;
	
	public SketchProcedure(SketchFunction sketch, int numTables) {
		super();
		this.sketch = sketch;
		this.numTables = numTables;
	}

	public ImageFeature call(ImageFeature feat) throws Exception {
		feat.computeSketch(this.sketch, this.numTables);
		return feat;
	}

}
