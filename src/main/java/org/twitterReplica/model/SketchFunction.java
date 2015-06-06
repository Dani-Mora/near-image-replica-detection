package org.twitterReplica.model;

import java.util.Random;

import org.twitterReplica.utils.ReplicaUtils;

import scala.Serializable;

public class SketchFunction implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8825088084150417052L;
	private static Random random = new Random();
	
	private double[] a;
	private double b;
	private int W;
	
	public SketchFunction(double[] a, double b, int W) {
		super();
		this.a = a;
		this.b = b;
		this.W = W;
	}

	public double[] getA() {
		return a;
	}

	public void setA(double[] a) {
		this.a = a;
	}

	public double getB() {
		return b;
	}

	public void setB(double b) {
		this.b = b;
	}

	public int getW() {
		return W;
	}

	public void setW(int w) {
		W = w;
	}
	
	/*
	 * 	Generates a new random LSH to sketch features
	 * 	@param length Length of the futures to be used
	 */
	public static SketchFunction getRandomLSHFunction(int length, int W) {
		double a[] = generateGaussianVector(length);
		double b = getUniformRandom(0, W);
		return new SketchFunction(a, b, W);
	}
	
	/*
	 * 	@param length Length of the resulting vector
	 * 	@return Random Gaussian distributed vector of the given length
	 */
	protected static double[] generateGaussianVector(int length) {
		double[] result = new double[length];
		for (int i = 0; i < length; ++i) {
			result[i] = getGaussianRandom();
		}
		return result;
	}
	
	/*
	 * 	Returns a random uniformly distributed double in the distribution [min, max)
	 */
	protected static double getUniformRandom(double min, double max) {
		double result = ReplicaUtils.getUniformRandomInc(min, max);
		while (result == max) ReplicaUtils.getUniformRandomInc(min, max);
		return result;
	}
	
	/*
	 * 	Returns a random Gaussian distributed double in the distribution [min, max]
	 */
	protected static double getGaussianRandom(double min, double max) {
		return min	+ (max - min) * getGaussianRandom();
	}
	
	/*
	 * 	Returns a random Gaussian distributed double in the distribution [0, 1]
	 */
	protected static double getGaussianRandom() {
		return Math.abs(random.nextGaussian());
	}
	
}
