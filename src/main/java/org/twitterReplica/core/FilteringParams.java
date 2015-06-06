package org.twitterReplica.core;

import org.twitterReplica.model.FilteringType;

import scala.Serializable;

public class FilteringParams implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private FilteringType filteringType;
	private double thresh;
	private boolean logScaleEnabled;
	
	public FilteringParams(FilteringType filteringType, double thresh,
			boolean logScaleEnabled) {
		super();
		this.filteringType = filteringType;
		this.thresh = thresh;
		this.logScaleEnabled = logScaleEnabled;
	}

	public FilteringType getFilteringType() {
		return filteringType;
	}

	public void setFilteringType(FilteringType filteringType) {
		this.filteringType = filteringType;
	}

	public double getThresh() {
		return thresh;
	}

	public void setThresh(double thresh) {
		this.thresh = thresh;
	}

	public boolean isLogScaleEnabled() {
		return logScaleEnabled;
	}

	public void setLogScaleEnabled(boolean logScaleEnabled) {
		this.logScaleEnabled = logScaleEnabled;
	}

}
