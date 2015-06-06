package org.twitterReplica.model.providers;

import java.security.ProviderException;
import java.util.List;

import org.apache.log4j.Logger;
import org.twitterReplica.exceptions.ConnectionException;
import org.twitterReplica.exceptions.InitializationException;
import org.twitterReplica.exceptions.NotImageResourceException;
import org.twitterReplica.exceptions.ParseResourceException;
import org.twitterReplica.model.ImageInfo;

public abstract class ResourceProvider<T> {

	// TODO: we may want to allow multiple listeners per event - check
	
	static Logger logger = Logger.getLogger(ResourceProvider.class.getName());
	
	private ImageResourceListener resourceReceivedListener;
	private ProviderDisconnectedListener disconnectedListener;
	
	public ResourceProvider() {
		super();
	}
	
	public void registerReceivedListener(ImageResourceListener listener) {
		this.resourceReceivedListener = listener;
	}
	
	public void registerDisconnectedListener(ProviderDisconnectedListener listener) {
		this.disconnectedListener = listener;
	}
	
	/*
	 * 	To be executed before a connection is started
	 */
	protected abstract void initialize() throws InitializationException;
	
	/*
	 * 	Starts the connection to the provider
	 */
	protected abstract void startConnection();
	
	/*
	 * 	Checks the state of the connection to the provider
	 * 	
	 * 	@return Whether we can still receive resources from the provider
	 */
	protected abstract boolean isConnectionDone();
	
	/*
	 * 	@return Next string formatted resource is returned
	 */
	protected abstract T getNextResource() throws ProviderException;
	
	/*
	 * 	@return Image resources extracted from the string formatted resource
	 */
	protected abstract List<ImageInfo> parseResource(T message) 
			throws ParseResourceException, NotImageResourceException;
	
	public void connect() throws ConnectionException, InitializationException {
		
		// Initialize state of the provider
		this.initialize();
		
		// Start connection to the provider
		this.startConnection();
		
		while(!this.isConnectionDone()) {
			
			// Parse resources
			T strResource = this.getNextResource();
			List<ImageInfo> resources;
			try {
				// Parse message
				resources = this.parseResource(strResource);
				// Notify from all resources extracted
				for (ImageInfo r : resources) {
					this.resourceReceivedListener.imageResReceived(r);
				}
			} catch (ParseResourceException e) {
				logger.info(e);
			} catch (NotImageResourceException e) {
				// Not an image - Nothing to do
				logger.info(e);
			}
		}
		
		// Disconnect
		this.disconnect();
		
		// Disconnedted event trigger
		this.disconnectedListener.onDisconnection();
	}
	
	/*
	 * 	Disconnects from the resource provider
	 */
	public abstract void disconnect() throws ProviderException;
	
}
