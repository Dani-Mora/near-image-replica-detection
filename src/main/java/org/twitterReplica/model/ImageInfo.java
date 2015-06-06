package org.twitterReplica.model;

import org.twitterReplica.model.providers.ProviderType;
import org.twitterReplica.model.replica.ReplicaType;

import scala.Serializable;

public class ImageInfo implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1792207854874212409L;
	
	// Image parameters
	private String path;
	private long id = -1;
	
	// Source parameters
	protected String resourceId;
	private ProviderType provider = ProviderType.UNKNOWN;
	
	// Replica parameters
	protected ReplicaType replicaType;
	protected long srcId;
	
	public ImageInfo(long id) {
		super();
		this.id = id;
	}
	
	public ImageInfo(String path, long id, ProviderType provider) {
		this(path, id);
		this.provider = provider;
	}
	
	public ImageInfo(String path, long id) {
		this(path);
		this.id = id;
	}
	
	public ImageInfo(String path) {
		super();
		this.path = path;
	}
	
	public ImageInfo(String path, long id, String resId, ProviderType provider,
			ReplicaType replicaType, long srcId) {
		this(path, id, resId, provider);
		this.replicaType = replicaType;
		this.srcId = srcId;
	}
	
	public ImageInfo(String path, long id, String resId, ProviderType provider) {
		this(path, id, provider);
		this.resourceId = resId;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public ReplicaType getReplicaType() {
		return replicaType;
	}

	public void setReplicaType(ReplicaType replicaType) {
		this.replicaType = replicaType;
	}

	public long getSrcId() {
		return srcId;
	}

	public void setSrcId(long srcId) {
		this.srcId = srcId;
	}

	public String getResourceId() {
		return resourceId;
	}

	public void setResourceId(String resourceId) {
		this.resourceId = resourceId;
	}

	public ProviderType getProvider() {
		return provider;
	}

	public void setProvider(ProviderType provider) {
		this.provider = provider;
	}
	
	public boolean isReplica() {
		return this.replicaType != null;
	}
	
}
