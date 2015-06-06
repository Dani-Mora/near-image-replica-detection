package org.twitterReplica.model;

/*
 * 	Level of persistence of the data
 * 		- Memory only: it stores data into memory without persisting it into disk. All querys are read from memory
 * 		- Disk only: it stores data into disk and reads from disk always.
 */
public enum PersistenceMode {
	MEMORY_ONLY, DISK_ONLY;
}
