package org.twitterReplica.core;

import scala.Serializable;

public class ReplicaConnection implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6073547545045610557L;
	
	private String firstParam;
	private String secondParam;
	private String thirdParam;
	
	/*
	 * 	Represents a connection to the eplica system. For disk based replica systems, 
	 * 	the three parameters relate to:
	 * 		1- HBase master URL
	 * 		2- Zookeeper port
	 * 		3- Zookeeper host
	 * 
	 * 	For memory based systems, the first parameters is used to point to the configuration file.
	 */
	public ReplicaConnection(String firstParam, String secondParam,
			String thirdParam) {
		super();
		this.firstParam = firstParam;
		this.secondParam = secondParam;
		this.thirdParam = thirdParam;
	}

	public String getFirstParam() {
		return firstParam;
	}

	public String getSecondParam() {
		return secondParam;
	}

	public String getThirdParam() {
		return thirdParam;
	}
	
}
