package org.twitterReplica.model.providers;

import java.security.ProviderException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.json.JSONArray;
import org.json.JSONObject;
import org.twitterReplica.exceptions.InitializationException;
import org.twitterReplica.exceptions.NotImageResourceException;
import org.twitterReplica.exceptions.ParseResourceException;
import org.twitterReplica.model.ImageInfo;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProvider extends ResourceProvider<String> {

	// TODO: store all images not first from tweets
	
	private BlockingQueue<String> queue;
	private Authentication auth;
	private com.twitter.hbc.core.endpoint.StatusesFilterEndpoint endpoint;
	private List<String> terms;
	
	private TwitterImgSize imgsize;
	private boolean crop = false;
	private boolean allowRetweets = false;
	private Client client;
	
	public TwitterProvider() {
		this.terms = new ArrayList<String>();
	}
	
	public void addKeyWord(String key) {
		this.terms.add(key);
	}
	
	public void setAuthentication(String consumerKey, String consumerSecret, String token, String tokenSecret) {
		this.auth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);
	}
	
	public TwitterImgSize getPreferredSize() {
		return this.imgsize;
	}
	
	public void setPreferredSize(TwitterImgSize size) {
		this.imgsize = size;
	}
	
	public void allowRetweets(boolean retweets) {
		this.allowRetweets = retweets;
	}
	
	/*
	 * 	Whether it is acceptable to get cropped versions of an image from its preferred
	 * 	size. If true, it will retrieve the url of the default (medium) version
	 * 
	 */
	public void enableCropped(boolean crop) {
		this.crop = crop;
	}
	
	@Override
	protected void initialize() throws InitializationException {
		// Initialize endpoint
		this.endpoint = new StatusesFilterEndpoint();
				
		// Initialize queues
		this.queue = new LinkedBlockingQueue<String>(10000);
				
		// Set keywords
		this.endpoint.trackTerms(terms);
			    
	    // Create a new BasicClient. By default gzip is enabled.
	    this.client = new ClientBuilder()
	      .hosts(Constants.STREAM_HOST)
	      .endpoint(endpoint)
	      .authentication(auth)
	      .processor(new StringDelimitedProcessor(queue))
	      .build();
	} 
	
	@Override
	protected void startConnection() {
		// Establish a connection
	    this.client.connect();
	}
	

	@Override
	protected boolean isConnectionDone() {
		return this.client.isDone();
	}
	
	@Override
	protected String getNextResource() throws ProviderException {
		try {
			return queue.take();
		} catch (InterruptedException e) {
			throw new ProviderException("Could not retrieve next tweet: " + e.getMessage());
		}
	}
	
	@Override
	protected List<ImageInfo> parseResource(String message) 
			throws NotImageResourceException, ParseResourceException {
		
		List<ImageInfo> result = new ArrayList<ImageInfo>();
		
		// Get media from tweet
		JSONObject tweet = new JSONObject(message);
    	boolean hasID = tweet.has("id_str");
    	boolean hasEntities = tweet.has("extended_entities");
    	boolean isRetweet = tweet.has("retweeted_status");
    	
    	// Check if is a retweet (and if we want to allow them) and whether it has media
    	
    	if (hasEntities && hasID && (this.allowRetweets || !this.allowRetweets && !isRetweet)) {
    		
        	String tweetID = tweet.getString("id_str");
        	JSONObject entities = tweet.getJSONObject("extended_entities");
        	boolean hasMedia = entities.has("media");
        	
        	if (hasMedia) {
        		
        		JSONArray mediaEntities = entities.getJSONArray("media");
        		
        		// List entities
        		for (int i = 0; i < mediaEntities.length(); ++i) {
        		
        			JSONObject currentMedia = mediaEntities.getJSONObject(i);
        			
        			if (currentMedia.getString("type").equals("photo")) {
        				
        				// Get preferred size string format
    					String size = this.getPreferredSize() == null ? "medium" : this.getPreferredSize().toString();
        				
    					// Lets select the size to download and its url
    					JSONObject sizes = currentMedia.getJSONObject("sizes");
    					String strUrl = null;
    					
    					if (sizes.has(size) && this.crop ||
    							sizes.has(size) && !this.crop && !sizes.getJSONObject(size).getString("resize").equals("crop")) {
    						// Preferred size available and fits cropping requirements
    						strUrl = currentMedia.getString("media_url_https") + ":" + size;
    					}
    					else {
    						strUrl = currentMedia.getString("media_url_https");
    					}
    					
    					// Build resource
    					long imgId = Long.valueOf(tweetID);
    					result.add(new ImageInfo(strUrl, imgId, tweetID, ProviderType.TWITTER));
    					break;
        			}
    			}
        	}
        	else {
        		throw new NotImageResourceException("The tweet " + tweetID + " does not contain an image");
        	}
    	}
    	else {
    		throw new NotImageResourceException("The tweet does not contain an ID or is not an image");
    	}

    	return result;	
	}

	@Override
	public void disconnect() throws ProviderException {
		this.client.stop();
	}

}
