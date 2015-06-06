package org.twitterReplica.model;

import scala.Serializable;

public class ImageMatch implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8091002906183467120L;
	
	private long queryId;
	private long matchedId;
	
	public ImageMatch(long queryId, long matchedId) {
		super();
		this.queryId = queryId;
		this.matchedId = matchedId;
	}

	public long getQueryId() {
		return queryId;
	}

	public void setQueryId(long queryId) {
		this.queryId = queryId;
	}

	public long getMatchedId() {
		return matchedId;
	}

	public void setMatchedId(long matchedId) {
		this.matchedId = matchedId;
	}
	
	@Override
    public int hashCode() {
		String first = String.valueOf(queryId);
		String second = String.valueOf(matchedId);
		String comb = first + "_" + second;
        return comb.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
       if (!(obj instanceof ImageMatch))
            return false;
        if (obj == this)
            return true;

        ImageMatch im = (ImageMatch) obj;
        return im.getMatchedId() == matchedId && im.getQueryId() == queryId;
    }
	
}
