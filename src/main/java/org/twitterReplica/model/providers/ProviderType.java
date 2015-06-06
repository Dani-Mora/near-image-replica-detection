package org.twitterReplica.model.providers;

import scala.Serializable;

public enum ProviderType implements Serializable {

	TWITTER ("Twitter"),
	INSTAGRAM ("Instagram"),
	DISK ("Disk"),
	UNKNOWN ("Unknown");
	
	private final String name;       
	
	private ProviderType(String s) {
	    name = s;
	}
	
	public boolean equalsName(String otherName){
	    return (otherName == null)? false:name.equals(otherName);
	}
	
	public static ProviderType fromString(String label) {
		for (ProviderType pt : ProviderType.values()) {
			if (pt.toString().equals(label)) {
				return pt;
			}
		}
		return null;
	}
	
	public String toString(){
	   return name;
	}
	
}
