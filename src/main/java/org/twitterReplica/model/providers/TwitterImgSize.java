package org.twitterReplica.model.providers;

public enum TwitterImgSize {
	
	SMALL ("small"),
	MEDIUM ("medium"),
	LARGE ("large"),
	THUMB ("thumb");
	
	private final String name;       
	
	private TwitterImgSize(String s) {
	    name = s;
	}
	
	public boolean equalsName(String otherName){
	    return (otherName == null)? false:name.equals(otherName);
	}
	
	public String toString(){
	   return name;
	}

}
