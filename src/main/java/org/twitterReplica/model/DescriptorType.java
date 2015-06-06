package org.twitterReplica.model;

public enum DescriptorType {
	
	ORB ("ORB"), 
	SIFT ("SIFT"), 
	FREAK ("FREAK"), 
	SURF ("SURF"), 
	BRIEF ("BRIEF"), 
	BRISK ("BRISK");
	
	private static double[] CUTS_SIFT = new double[] { 5, 20, 46};
	private static double[] CUTS_FREAK = new double[] { 51, 118, 192};
	private static double[] CUTS_BRISK = new double[] { 29, 127, 229};
	private static double[] CUTS_BRIEF = new double[] { 58, 127, 195};
	private static double[] CUTS_ORB = new double[] { 40, 104, 168};
	private static double[] CUTS_SURF = new double[] { -0.05, 0.005, 0.04 };
	
	private final String name;       
	
	private DescriptorType(String s) {
	    name = s;
	}
	
	public boolean equalsName(String otherName){
	    return (otherName == null)? false:name.equals(otherName);
	}
	
	public String toString(){
	   return name;
	}
	
	public static double getMinimumValue(DescriptorType type) {
		switch(type) {
			case SURF:
				return -1.0;
			default:
				return 0.0;
		}
	}
	
	public static double getMaximumValue(DescriptorType type) {
		switch(type) {
			case SURF:
				return 1.0;
			default:
				return 255.0;
		}
	}
	
	public static int getSize(DescriptorType type) {
		switch(type) {
			case SIFT:
				return 128;
			case ORB:
				return 32;
			case BRIEF:
				return 32;
			default:
				return 64;
		}
	}
	
	public static double[] getCuts(DescriptorType type) {
		switch(type) {
			case SIFT:
				return CUTS_SIFT;
			case SURF:
				return CUTS_SURF;
			case BRIEF:
				return CUTS_BRIEF;
			case BRISK:
				return CUTS_BRISK;
			case ORB:
				return CUTS_ORB;
			default:
				return CUTS_FREAK;
		}
	}
	
}
